use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use coral_api::v1::QueryGuideRequirement;

use crate::surface::{SqlGuideValue, TaskId};

const MAX_RETAINED_TASKS: usize = 1_024;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct GuideResourceKey {
    schema_name: String,
    resource_name: String,
}

#[derive(Default)]
struct TaskGuideState {
    resources: HashMap<GuideResourceKey, String>,
    sql_gate: Arc<tokio::sync::Mutex<()>>,
    last_used: u64,
}

#[derive(Default)]
struct GuideBlockStateInner {
    tasks: HashMap<TaskId, TaskGuideState>,
    clock: u64,
}

impl GuideBlockStateInner {
    fn touch_task(&mut self, task_id: TaskId, max_tasks: usize) -> &mut TaskGuideState {
        self.clock = self.clock.saturating_add(1);
        self.tasks.entry(task_id).or_default().last_used = self.clock;
        while self.tasks.len() > max_tasks {
            let Some(oldest) = self
                .tasks
                .iter()
                .filter(|(candidate_id, task)| {
                    **candidate_id != task_id && Arc::strong_count(&task.sql_gate) == 1
                })
                .min_by_key(|(_, task)| task.last_used)
                .map(|(candidate_id, _)| *candidate_id)
            else {
                break;
            };
            self.tasks.remove(&oldest);
        }
        self.tasks
            .get_mut(&task_id)
            .expect("the touched guide-block task remains retained")
    }
}

pub(crate) struct GuideBlockState {
    inner: Mutex<GuideBlockStateInner>,
    max_tasks: usize,
}

impl Default for GuideBlockState {
    fn default() -> Self {
        Self {
            inner: Mutex::new(GuideBlockStateInner::default()),
            max_tasks: MAX_RETAINED_TASKS,
        }
    }
}

impl GuideBlockState {
    #[cfg(test)]
    fn with_max_tasks(max_tasks: usize) -> Self {
        assert!(max_tasks > 0, "guide block task limit must be positive");
        Self {
            max_tasks,
            ..Self::default()
        }
    }

    pub(crate) fn sql_gate(
        &self,
        task_id: TaskId,
    ) -> Result<Arc<tokio::sync::Mutex<()>>, tonic::Status> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_poisoned| tonic::Status::internal("guide block state lock poisoned"))?;
        Ok(Arc::clone(
            &inner.touch_task(task_id, self.max_tasks).sql_gate,
        ))
    }

    pub(crate) fn newly_required_guides(
        &self,
        task_id: TaskId,
        requirements: Vec<QueryGuideRequirement>,
    ) -> Result<Vec<SqlGuideValue>, tonic::Status> {
        let mut candidates = BTreeMap::new();
        for requirement in requirements {
            candidates.insert(
                GuideResourceKey {
                    schema_name: requirement.schema_name,
                    resource_name: requirement.resource_name,
                },
                requirement.guide,
            );
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|_poisoned| tonic::Status::internal("guide block state lock poisoned"))?;
        let seen = &mut inner.touch_task(task_id, self.max_tasks).resources;
        let mut newly_required = Vec::new();
        for (key, guide) in candidates {
            if seen.get(&key) == Some(&guide) {
                continue;
            }
            seen.insert(key.clone(), guide.clone());
            newly_required.push(SqlGuideValue::new(
                key.schema_name,
                key.resource_name,
                guide,
            ));
        }
        Ok(newly_required)
    }

    pub(crate) fn clear_task(&self, task_id: TaskId) -> Result<(), tonic::Status> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_poisoned| tonic::Status::internal("guide block state lock poisoned"))?;
        inner.tasks.remove(&task_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::QueryGuideRequirement;

    use super::GuideBlockState;
    use crate::surface::TaskId;

    fn task_id(value: u128) -> TaskId {
        TaskId::from_uuid_str(&uuid::Uuid::from_u128(value).to_string()).expect("task id")
    }

    fn requirement(guide: &str) -> QueryGuideRequirement {
        QueryGuideRequirement {
            schema_name: "slack".to_string(),
            resource_name: "channels".to_string(),
            guide: guide.to_string(),
        }
    }

    fn required(state: &GuideBlockState, task_id: TaskId, guide: &str) -> usize {
        state
            .newly_required_guides(task_id, vec![requirement(guide)])
            .expect("guide block state")
            .len()
    }

    #[test]
    fn guide_changes_and_task_boundaries_require_another_read() {
        let state = GuideBlockState::default();
        let first_task = task_id(1);

        assert_eq!(required(&state, first_task, "Use search."), 1);
        assert_eq!(required(&state, first_task, "Use search."), 0);
        assert_eq!(required(&state, first_task, "Use lookup."), 1);
        assert_eq!(required(&state, task_id(2), "Use lookup."), 1);
        state.clear_task(first_task).expect("clear task");
        assert_eq!(required(&state, first_task, "Use lookup."), 1);
    }

    #[test]
    fn sql_gate_serializes_calls_for_the_same_task() {
        let state = GuideBlockState::default();
        let first_gate = state.sql_gate(task_id(1)).expect("first task gate");
        let same_task_gate = state.sql_gate(task_id(1)).expect("same task gate");
        let other_task_gate = state.sql_gate(task_id(2)).expect("other task gate");

        assert!(Arc::ptr_eq(&first_gate, &same_task_gate));
        let first_call = first_gate.try_lock_owned().expect("first SQL call");
        assert!(
            Arc::clone(&same_task_gate).try_lock_owned().is_err(),
            "overlapping SQL call in the same task must wait"
        );
        assert!(
            other_task_gate.try_lock_owned().is_ok(),
            "different tasks must remain independent"
        );
        drop(first_call);
        assert!(
            same_task_gate.try_lock_owned().is_ok(),
            "same-task SQL call must continue after the previous response"
        );
    }

    #[test]
    fn evicts_the_least_recently_used_task() {
        let state = GuideBlockState::with_max_tasks(2);
        let first_task = task_id(1);
        let second_task = task_id(2);
        let third_task = task_id(3);

        assert_eq!(required(&state, first_task, "Use search."), 1);
        assert_eq!(required(&state, second_task, "Use search."), 1);
        drop(state.sql_gate(first_task).expect("touch first task"));
        drop(state.sql_gate(third_task).expect("insert third task"));

        assert_eq!(
            (
                required(&state, first_task, "Use search."),
                required(&state, second_task, "Use search.")
            ),
            (0, 1)
        );
    }

    #[test]
    fn reclaims_overflow_after_active_sql_finishes() {
        let state = GuideBlockState::with_max_tasks(1);
        let active_task = task_id(1);
        let other_task = task_id(2);
        let active_gate = state.sql_gate(active_task).expect("active task gate");
        let active_call = active_gate.try_lock_owned().expect("active SQL call");

        drop(state.sql_gate(other_task).expect("other task gate"));

        let inner = state.inner.lock().expect("guide block state");
        assert!(
            inner.tasks.contains_key(&active_task),
            "an active task must remain retained"
        );
        assert_eq!(
            inner.tasks.len(),
            2,
            "the limit may be exceeded instead of evicting the active task"
        );
        drop(inner);
        drop(active_call);

        drop(state.sql_gate(other_task).expect("reuse other task gate"));
        let inner = state.inner.lock().expect("guide block state");
        assert_eq!(inner.tasks.len(), 1);
        assert!(inner.tasks.contains_key(&other_task));
    }
}
