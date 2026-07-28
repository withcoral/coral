use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use coral_api::v1::QueryGuideVersion;

use crate::surface::{SqlGuideValue, TaskId};

const MAX_RETAINED_TASKS: usize = 1_024;

#[derive(Eq, Hash, PartialEq)]
struct ShownGuideVersion {
    schema_name: String,
    resource_name: String,
    guide_fingerprint: String,
}

#[derive(Default)]
struct TaskGuideState {
    guides: HashSet<ShownGuideVersion>,
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
        if self.tasks.len() > max_tasks {
            let oldest = self
                .tasks
                .iter()
                .filter(|(candidate_id, _)| **candidate_id != task_id)
                .min_by_key(|(_, task)| task.last_used)
                .map(|(candidate_id, _)| *candidate_id)
                .expect("an overflowing guide state has an older task");
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

    pub(crate) fn shown_guide_versions(
        &self,
        task_id: TaskId,
    ) -> Result<Vec<QueryGuideVersion>, tonic::Status> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_poisoned| tonic::Status::internal("guide block state lock poisoned"))?;
        let task = inner.touch_task(task_id, self.max_tasks);
        let mut versions = task
            .guides
            .iter()
            .map(|guide| QueryGuideVersion {
                schema_name: guide.schema_name.clone(),
                resource_name: guide.resource_name.clone(),
                guide_fingerprint: guide.guide_fingerprint.clone(),
            })
            .collect::<Vec<_>>();
        versions.sort_unstable_by(|left, right| {
            (
                &left.schema_name,
                &left.resource_name,
                &left.guide_fingerprint,
            )
                .cmp(&(
                    &right.schema_name,
                    &right.resource_name,
                    &right.guide_fingerprint,
                ))
        });
        Ok(versions)
    }

    pub(crate) fn record_guides(
        &self,
        task_id: TaskId,
        guides: &[SqlGuideValue],
    ) -> Result<(), tonic::Status> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_poisoned| tonic::Status::internal("guide block state lock poisoned"))?;
        let shown_guides = &mut inner.touch_task(task_id, self.max_tasks).guides;
        for guide in guides {
            shown_guides.insert(ShownGuideVersion {
                schema_name: guide.schema.clone(),
                resource_name: guide.resource.clone(),
                guide_fingerprint: guide.fingerprint.clone(),
            });
        }
        Ok(())
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
    use super::GuideBlockState;
    use crate::surface::{SqlGuideValue, TaskId};

    fn task_id(value: u128) -> TaskId {
        TaskId::from_uuid_str(&uuid::Uuid::from_u128(value).to_string()).expect("task id")
    }

    fn guide(text: &str, fingerprint: &str) -> SqlGuideValue {
        SqlGuideValue::new(
            "slack".to_string(),
            "channels".to_string(),
            text.to_string(),
            fingerprint.to_string(),
        )
    }

    #[test]
    fn records_each_guide_version_per_task_and_resource() {
        let state = GuideBlockState::default();
        let first_task = task_id(1);

        state
            .record_guides(first_task, &[guide("Use search.", "v1:first")])
            .expect("record first guide");
        assert_eq!(
            state
                .shown_guide_versions(first_task)
                .expect("first versions")
                .into_iter()
                .map(|guide| guide.guide_fingerprint)
                .collect::<Vec<_>>(),
            ["v1:first"]
        );

        state
            .record_guides(first_task, &[guide("Use lookup.", "v1:second")])
            .expect("record changed guide");
        assert_eq!(
            state
                .shown_guide_versions(first_task)
                .expect("updated versions")
                .into_iter()
                .map(|guide| guide.guide_fingerprint)
                .collect::<Vec<_>>(),
            ["v1:first", "v1:second"]
        );
        assert!(
            state
                .shown_guide_versions(task_id(2))
                .expect("other task versions")
                .is_empty()
        );
    }

    #[test]
    fn evicts_the_least_recently_used_task() {
        let state = GuideBlockState::with_max_tasks(2);
        let first_task = task_id(1);
        let second_task = task_id(2);
        let third_task = task_id(3);

        state
            .record_guides(first_task, &[guide("First.", "v1:first")])
            .expect("first task");
        state
            .record_guides(second_task, &[guide("Second.", "v1:second")])
            .expect("second task");
        state
            .shown_guide_versions(first_task)
            .expect("touch first task");
        state
            .shown_guide_versions(third_task)
            .expect("insert third task");

        assert_eq!(
            (
                state
                    .shown_guide_versions(first_task)
                    .expect("retained task")
                    .len(),
                state
                    .shown_guide_versions(second_task)
                    .expect("evicted task")
                    .len(),
            ),
            (1, 0)
        );
    }
}
