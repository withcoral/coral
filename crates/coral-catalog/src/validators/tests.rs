use std::cell::Cell;

use super::*;

const OUTPUT: SchemaValidationDirection = SchemaValidationDirection::Output;
const NEUTRAL: SchemaValidationDirection = SchemaValidationDirection::Neutral;

fn root(name: &str) -> SchemaId {
    SchemaId::for_source(name)
}

/// A validator that no other here is the same allocation as.
fn compiled() -> Compiled {
    compile(&serde_json::json!({"type": "object"}))
}

/// A bundle that compiles to a validator of objects, and the count of the
/// times it was built.
fn counted_bundle(built: &Cell<usize>) -> impl Fn() -> Option<Value> {
    || {
        built.set(built.get() + 1);
        Some(serde_json::json!({"type": "object"}))
    }
}

/// What a graph with a place in `store` holds of its validators.
fn placed(store: &ValidatorStore) -> Validators {
    let mut validators = Validators::default();
    validators.keep_in(store);
    validators
}

// Catches: a validator built again on a later call, and one graph, root or
// direction served the validator of another. Two graphs name different
// schemas by one root identity, so the last would check a response against
// another Provider's schema.
#[test]
fn a_validator_is_built_once_for_each_graph_root_and_direction() {
    let store = ValidatorStore::new(ValidatorStore::BUDGET_BYTES);
    let graph = placed(&store);
    let built = Cell::new(0);
    let bundle = counted_bundle(&built);

    let first = graph
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    assert_eq!(built.get(), 1);
    for call in ["second", "third"] {
        let again = graph
            .validator(&root("a"), OUTPUT, &bundle)
            .expect("the schema compiles");
        assert!(Arc::ptr_eq(&first, &again), "{call} call");
    }
    assert_eq!(built.get(), 1, "later calls build nothing");

    let other_graph = placed(&store);
    for (name, validators, other_root, direction) in [
        ("another direction", &graph, root("a"), NEUTRAL),
        ("another root", &graph, root("b"), OUTPUT),
        ("another graph", &other_graph, root("a"), OUTPUT),
    ] {
        let before = built.get();
        let other = validators
            .validator(&other_root, direction, &bundle)
            .expect("the schema compiles");
        assert_eq!(built.get() - before, 1, "{name} builds its own");
        assert!(!Arc::ptr_eq(&first, &other), "{name}");
    }
}

// Catches: a graph that the cache does not keep holding validators that no
// budget counts, and a copy of a graph taking the place of the graph it was
// copied from, so that dropping the copy empties the original's place.
#[test]
fn a_graph_with_no_place_in_a_store_builds_on_every_use_and_keeps_nothing() {
    let store = ValidatorStore::new(ValidatorStore::BUDGET_BYTES);
    let graph = placed(&store);
    let built = Cell::new(0);
    let bundle = counted_bundle(&built);
    let kept = graph
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    let weight = store.read().weight_bytes;

    let copy = graph.clone();
    assert_eq!(copy, graph);
    for (name, validators) in [
        ("a new graph", Validators::default()),
        ("a copy of a graph with a place", copy),
    ] {
        let before = built.get();
        for _call in 0..2 {
            let own = validators
                .validator(&root("a"), OUTPUT, &bundle)
                .expect("the schema compiles");
            assert!(!Arc::ptr_eq(&kept, &own), "{name}");
        }
        assert_eq!(built.get() - before, 2, "{name} builds on each call");
        drop(validators);
        assert_eq!(store.read().weight_bytes, weight, "{name} keeps nothing");
    }

    let before = built.get();
    let again = graph
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    assert!(Arc::ptr_eq(&kept, &again), "the graph still has its place");
    assert_eq!(built.get(), before);
}

// Catches: a schema that does not compile being bundled and compiled on every
// call, and a root with no schema leaving an entry behind.
#[test]
fn a_schema_that_does_not_compile_is_built_once_and_a_missing_root_keeps_nothing() {
    let store = ValidatorStore::new(ValidatorStore::BUDGET_BYTES);
    let graph = placed(&store);
    let built = Cell::new(0);
    let invalid = || {
        built.set(built.get() + 1);
        Some(serde_json::json!({"type": 12}))
    };
    for call in ["first", "second"] {
        assert!(
            graph.validator(&root("a"), OUTPUT, invalid).is_none(),
            "{call} call"
        );
    }
    assert_eq!(built.get(), 1, "the outcome is kept");

    let missing = || {
        built.set(built.get() + 1);
        None
    };
    let before = (built.get(), store.read().by_order.len());
    for call in ["first", "second"] {
        assert!(
            graph.validator(&root("b"), OUTPUT, missing).is_none(),
            "{call} call"
        );
    }
    assert_eq!(built.get() - before.0, 2, "nothing is kept for no schema");
    assert_eq!(store.read().by_order.len(), before.1);
}

/// One step of a budget case: a graph and one of its roots.
#[derive(Clone, Copy)]
enum Step {
    /// The root's validator is kept at this weight.
    Keep(GraphId, &'static str, usize),
    /// The validator kept for the root is used again.
    Use(GraphId, &'static str),
    /// The graph is dropped.
    Forget(GraphId),
}

struct BudgetCase {
    name: &'static str,
    budget_bytes: usize,
    steps: &'static [Step],
    kept: &'static [(GraphId, &'static str)],
}

const BUDGET_CASES: [BudgetCase; 13] = [
    BudgetCase {
        name: "two validators within the budget",
        budget_bytes: 20,
        steps: &[Step::Keep(1, "a", 10), Step::Keep(1, "b", 10)],
        kept: &[(1, "a"), (1, "b")],
    },
    BudgetCase {
        name: "a validator over the budget evicts the one used longest ago",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(1, "b", 10),
            Step::Keep(1, "c", 1),
        ],
        kept: &[(1, "b"), (1, "c")],
    },
    BudgetCase {
        name: "a validator used again outlives one that was not",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(1, "b", 10),
            Step::Use(1, "a"),
            Step::Keep(1, "c", 1),
        ],
        kept: &[(1, "a"), (1, "c")],
    },
    BudgetCase {
        name: "the one validator not used again leaves before those that were",
        budget_bytes: 30,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(1, "b", 10),
            Step::Keep(2, "c", 10),
            Step::Use(1, "a"),
            Step::Use(1, "b"),
            Step::Keep(2, "d", 10),
        ],
        kept: &[(1, "a"), (1, "b"), (2, "d")],
    },
    BudgetCase {
        name: "validators used again leave in the order of their last use",
        budget_bytes: 30,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(1, "b", 10),
            Step::Keep(2, "c", 10),
            Step::Use(1, "b"),
            Step::Use(2, "c"),
            Step::Use(1, "a"),
            Step::Keep(2, "d", 20),
        ],
        kept: &[(1, "a"), (2, "d")],
    },
    BudgetCase {
        name: "one validator evicts as many as it needs room for",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 5),
            Step::Keep(2, "a", 5),
            Step::Keep(1, "c", 18),
        ],
        kept: &[(1, "c")],
    },
    BudgetCase {
        name: "a validator of the whole budget is kept alone",
        budget_bytes: 20,
        steps: &[Step::Keep(1, "a", 10), Step::Keep(2, "b", 20)],
        kept: &[(2, "b")],
    },
    BudgetCase {
        name: "a validator over the whole budget is not kept and evicts nothing",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(2, "b", 21),
            Step::Keep(2, "c", 10),
        ],
        kept: &[(1, "a"), (2, "c")],
    },
    BudgetCase {
        name: "a budget of zero keeps nothing",
        budget_bytes: 0,
        steps: &[Step::Keep(1, "a", 1)],
        kept: &[],
    },
    BudgetCase {
        name: "two graphs share the budget, and a graph's new validator evicts its own older one",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(2, "a", 10),
            Step::Keep(1, "b", 1),
        ],
        kept: &[(2, "a"), (1, "b")],
    },
    BudgetCase {
        name: "a graph's new validator evicts an idle one of another graph before its own in use",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 10),
            Step::Keep(2, "a", 10),
            Step::Use(1, "a"),
            Step::Keep(1, "b", 1),
        ],
        kept: &[(1, "a"), (1, "b")],
    },
    BudgetCase {
        name: "a dropped graph's validators leave and free their weight for another graph's",
        budget_bytes: 20,
        steps: &[
            Step::Keep(1, "a", 5),
            Step::Keep(2, "a", 5),
            Step::Keep(1, "b", 10),
            Step::Forget(1),
            Step::Keep(3, "a", 15),
        ],
        kept: &[(2, "a"), (3, "a")],
    },
    BudgetCase {
        name: "a dropped graph with no validators changes nothing",
        budget_bytes: 20,
        steps: &[Step::Keep(1, "a", 10), Step::Forget(2), Step::Forget(2)],
        kept: &[(1, "a")],
    },
];

// Catches: a store that grows past its budget, one that lets go of a
// validator still in use before an idle one, one that a validator over the
// whole budget empties, one that evicts within a graph and not across them,
// and one that keeps the validators of a graph that is gone.
#[test]
fn the_byte_budget_evicts_the_validators_used_longest_ago_whatever_their_graph() {
    for case in BUDGET_CASES {
        let mut held = Held::default();
        let mut made = Vec::new();
        for &step in case.steps {
            match step {
                Step::Keep(graph, name, weight_bytes) => {
                    let validator = compiled();
                    let (used, released) = held.keep(
                        graph,
                        (OUTPUT, root(name)),
                        validator.clone(),
                        weight_bytes,
                        case.budget_bytes,
                    );
                    assert!(
                        same(&used, &validator),
                        "{}: the caller uses the validator it built",
                        case.name
                    );
                    assert!(
                        released.iter().all(|released| !same(released, &validator)),
                        "{}: the caller's validator is never released to it",
                        case.name
                    );
                    made.push((graph, name, weight_bytes));
                }
                Step::Use(graph, name) => assert!(
                    held.mark_used(graph, &(OUTPUT, root(name))).is_some(),
                    "{}: {name} of graph {graph} is kept",
                    case.name
                ),
                Step::Forget(graph) => drop(held.forget(graph)),
            }
        }

        let kept = |graph: GraphId, name: &str| {
            held.by_graph
                .get(&graph)
                .is_some_and(|roots| roots.contains_key(&(OUTPUT, root(name))))
        };
        for &(graph, name, _weight_bytes) in &made {
            assert_eq!(
                kept(graph, name),
                case.kept.contains(&(graph, name)),
                "{}: whether {name} of graph {graph} is kept",
                case.name
            );
        }
        let kept_weight = made
            .iter()
            .filter(|(graph, name, _weight_bytes)| kept(*graph, name))
            .map(|(_graph, _name, weight_bytes)| weight_bytes)
            .sum::<usize>();
        assert_eq!(held.weight_bytes, kept_weight, "{}", case.name);
        assert!(held.weight_bytes <= case.budget_bytes, "{}", case.name);
        assert_eq!(
            held.by_order.len(),
            case.kept.len(),
            "{}: each validator is in the use order once",
            case.name
        );
        assert!(
            held.by_graph.values().all(|roots| !roots.is_empty()),
            "{}: no graph is kept without a validator",
            case.name
        );
    }
}

// Catches: a use that took its tick first and wrote it last moving its
// validator's last use backwards, so that the store lets go of a validator
// used after the one it keeps.
#[test]
fn a_use_that_writes_its_tick_late_does_not_make_its_validator_look_older() {
    let graph = 1;
    let (busy, idle) = ((OUTPUT, root("busy")), (OUTPUT, root("idle")));
    let kept_idle = compiled();
    let mut held = Held::default();
    drop(held.keep(graph, busy.clone(), compiled(), 1, usize::MAX));
    drop(held.keep(graph, idle.clone(), kept_idle.clone(), 1, usize::MAX));

    // Three uses take their ticks in this order. The first to take its tick
    // is the last to write it.
    let (early, between, late) = (held.tick(), held.tick(), held.tick());
    drop(held.mark_used_at(graph, &busy, late));
    drop(held.mark_used_at(graph, &idle, between));
    drop(held.mark_used_at(graph, &busy, early));

    let released = held
        .take_least_recently_used()
        .expect("two validators are kept");
    assert!(
        same(&released, &kept_idle),
        "the validator used longest ago leaves first"
    );
}

fn same(left: &Compiled, right: &Compiled) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
}

// Catches: the validators of a graph that is gone holding the store's budget
// until they age out, and a graph's drop taking another graph's validators
// with it.
#[test]
fn a_dropped_graph_gives_up_its_place_and_its_validators_leave_the_store() {
    let store = ValidatorStore::new(ValidatorStore::BUDGET_BYTES);
    let built = Cell::new(0);
    let bundle = counted_bundle(&built);
    let (leaving, staying) = (placed(&store), placed(&store));
    let stays = staying
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    let one = store.read().weight_bytes;
    for name in ["a", "b"] {
        leaving
            .validator(&root(name), OUTPUT, &bundle)
            .expect("the schema compiles");
    }
    assert_eq!(store.read().weight_bytes, 3 * one);

    drop(leaving);
    assert_eq!(
        store.read().weight_bytes,
        one,
        "the weight of the dropped graph's validators is free"
    );
    assert_eq!(store.read().by_order.len(), 1);
    let before = built.get();
    let again = staying
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    assert!(Arc::ptr_eq(&stays, &again), "the other graph keeps its own");
    assert_eq!(built.get(), before);

    drop(staying);
    assert_eq!(store.read().weight_bytes, 0);
    assert_eq!(store.read().by_graph.len(), 0);
}

// Catches: a build that lost the race for a root replacing the validator
// that a caller already uses, weighing the root twice, or dropping its own
// validator inside the lock.
#[test]
fn a_second_validator_for_a_kept_root_is_released_and_the_first_stays() {
    let (budget, weight) = (25, 10);
    let mut held = Held::default();
    let [contended, idle, next] = ["contended", "idle", "next"].map(|name| (OUTPUT, root(name)));
    let (first, other, late) = (compiled(), compiled(), compiled());
    let (used, released) = held.keep(1, contended.clone(), first.clone(), weight, budget);
    assert!(same(&used, &first));
    assert_eq!(released.len(), 0, "the first validator displaces nothing");
    let (_used, released) = held.keep(2, idle.clone(), other.clone(), weight, budget);
    assert_eq!(released.len(), 0, "both validators are within the budget");

    // A concurrent build for the first root finishes late.
    let (used, released) = held.keep(1, contended.clone(), late.clone(), weight, budget);
    assert!(same(&used, &first), "the first validator is the one in use");
    assert!(
        matches!(&released[..], [only] if same(only, &late)),
        "the caller drops the late validator, and nothing else"
    );
    assert_eq!(held.weight_bytes, 2 * weight);
    assert_eq!(held.by_order.len(), 2);

    // The late build used the entry as a hit does, so the idle one is evicted first.
    let (_used, released) = held.keep(1, next, compiled(), weight, budget);
    assert!(matches!(&released[..], [only] if same(only, &other)));
    assert!(held.mark_used(1, &contended).is_some());
    assert!(held.mark_used(2, &idle).is_none());
}
