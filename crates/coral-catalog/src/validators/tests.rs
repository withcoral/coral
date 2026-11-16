use std::cell::Cell;

use super::*;

const OUTPUT: SchemaValidationDirection = SchemaValidationDirection::Output;
const NEUTRAL: SchemaValidationDirection = SchemaValidationDirection::Neutral;

fn root(name: &str) -> SchemaId {
    SchemaId::for_source(name)
}

/// A validator that no other here is the same allocation as.
fn compiled() -> Compiled {
    let validator = jsonschema::validator_for(&serde_json::json!({"type": "object"}));
    validator.ok().map(Arc::new)
}

/// A bundle that compiles to a validator of objects, and the count of the
/// times it was built.
fn counted_bundle(built: &Cell<usize>) -> impl Fn() -> Option<Value> {
    || {
        built.set(built.get() + 1);
        Some(serde_json::json!({"type": "object"}))
    }
}

// Catches: a validator built again on a later call, one direction served the
// validator of the other, and a copy of a graph sharing or copying what the
// graph compiled.
#[test]
fn a_validator_is_built_once_for_each_root_and_direction() {
    let validators = Validators::default();
    let built = Cell::new(0);
    let bundle = counted_bundle(&built);

    let first = validators
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    assert_eq!(built.get(), 1);
    for call in ["second", "third"] {
        let again = validators
            .validator(&root("a"), OUTPUT, &bundle)
            .expect("the schema compiles");
        assert!(Arc::ptr_eq(&first, &again), "{call} call");
    }
    assert_eq!(built.get(), 1, "later calls build nothing");

    for (name, other_root, direction) in [
        ("another direction", root("a"), NEUTRAL),
        ("another root", root("b"), OUTPUT),
    ] {
        let before = built.get();
        let other = validators
            .validator(&other_root, direction, &bundle)
            .expect("the schema compiles");
        assert_eq!(built.get() - before, 1, "{name} builds its own");
        assert!(!Arc::ptr_eq(&first, &other), "{name}");
    }

    let copy = validators.clone();
    assert_eq!(copy, validators);
    let before = built.get();
    let copied = copy
        .validator(&root("a"), OUTPUT, &bundle)
        .expect("the schema compiles");
    assert_eq!(built.get() - before, 1, "a copy builds its own");
    assert!(!Arc::ptr_eq(&first, &copied));
}

// Catches: a schema that does not compile being bundled and compiled on every
// call, and a root with no schema leaving an entry behind.
#[test]
fn a_schema_that_does_not_compile_is_built_once_and_a_missing_root_keeps_nothing() {
    let validators = Validators::default();
    let built = Cell::new(0);
    let invalid = || {
        built.set(built.get() + 1);
        Some(serde_json::json!({"type": 12}))
    };
    for call in ["first", "second"] {
        assert!(
            validators.validator(&root("a"), OUTPUT, invalid).is_none(),
            "{call} call"
        );
    }
    assert_eq!(built.get(), 1, "the outcome is kept");

    let missing = || {
        built.set(built.get() + 1);
        None
    };
    let before = (built.get(), validators.held().by_key.len());
    for call in ["first", "second"] {
        assert!(
            validators.validator(&root("b"), OUTPUT, missing).is_none(),
            "{call} call"
        );
    }
    assert_eq!(built.get() - before.0, 2, "nothing is kept for no schema");
    assert_eq!(validators.held().by_key.len(), before.1);
}

struct BudgetCase {
    name: &'static str,
    budget_bytes: usize,
    /// Roots in order, each with the weight it is kept at, or `None` for a
    /// use of a root that an earlier step named.
    steps: &'static [(&'static str, Option<usize>)],
    kept: &'static [&'static str],
}

const BUDGET_CASES: [BudgetCase; 7] = [
    BudgetCase {
        name: "two validators within the budget",
        budget_bytes: 20,
        steps: &[("a", Some(10)), ("b", Some(10))],
        kept: &["a", "b"],
    },
    BudgetCase {
        name: "a validator over the budget evicts the one used longest ago",
        budget_bytes: 20,
        steps: &[("a", Some(10)), ("b", Some(10)), ("c", Some(1))],
        kept: &["b", "c"],
    },
    BudgetCase {
        name: "a validator used again outlives one that was not",
        budget_bytes: 20,
        steps: &[
            ("a", Some(10)),
            ("b", Some(10)),
            ("a", None),
            ("c", Some(1)),
        ],
        kept: &["a", "c"],
    },
    BudgetCase {
        name: "one validator evicts as many as it needs room for",
        budget_bytes: 20,
        steps: &[("a", Some(5)), ("b", Some(5)), ("c", Some(18))],
        kept: &["c"],
    },
    BudgetCase {
        name: "a validator of the whole budget is kept alone",
        budget_bytes: 20,
        steps: &[("a", Some(10)), ("b", Some(20))],
        kept: &["b"],
    },
    BudgetCase {
        name: "a validator over the whole budget is not kept and evicts nothing",
        budget_bytes: 20,
        steps: &[("a", Some(10)), ("b", Some(21)), ("c", Some(10))],
        kept: &["a", "c"],
    },
    BudgetCase {
        name: "a budget of zero keeps nothing",
        budget_bytes: 0,
        steps: &[("a", Some(1))],
        kept: &[],
    },
];

// Catches: a graph whose validators grow past their budget, one that lets go
// of a validator still in use before an idle one, and one that a validator
// over the whole budget empties.
#[test]
fn the_byte_budget_evicts_the_validators_used_longest_ago() {
    for case in BUDGET_CASES {
        let mut held = Held::default();
        let mut made = Vec::new();
        for &(name, weight_bytes) in case.steps {
            let key = (OUTPUT, root(name));
            let Some(weight_bytes) = weight_bytes else {
                assert!(held.mark_used(&key).is_some(), "{}: {name}", case.name);
                continue;
            };
            let validator = compiled();
            let (used, released) =
                held.keep(key, validator.clone(), weight_bytes, case.budget_bytes);
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
            made.push((name, weight_bytes));
        }

        let kept = |name: &str| held.by_key.contains_key(&(OUTPUT, root(name)));
        for (name, _weight_bytes) in &made {
            assert_eq!(
                kept(name),
                case.kept.contains(name),
                "{}: whether {name} is kept",
                case.name
            );
        }
        let kept_weight = made
            .iter()
            .filter(|(name, _weight_bytes)| kept(name))
            .map(|(_name, weight_bytes)| weight_bytes)
            .sum::<usize>();
        assert_eq!(held.weight_bytes, kept_weight, "{}", case.name);
        assert!(held.weight_bytes <= case.budget_bytes, "{}", case.name);
        assert_eq!(
            held.by_last_use.len(),
            held.by_key.len(),
            "{}: each validator is in the use order once",
            case.name
        );
    }
}

fn same(left: &Compiled, right: &Compiled) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
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
    let (used, released) = held.keep(contended.clone(), first.clone(), weight, budget);
    assert!(same(&used, &first));
    assert_eq!(released.len(), 0, "the first validator displaces nothing");
    let (_used, released) = held.keep(idle.clone(), other.clone(), weight, budget);
    assert_eq!(released.len(), 0, "both validators are within the budget");

    // A concurrent build for the first root finishes late.
    let (used, released) = held.keep(contended.clone(), late.clone(), weight, budget);
    assert!(same(&used, &first), "the first validator is the one in use");
    assert!(
        matches!(&released[..], [only] if same(only, &late)),
        "the caller drops the late validator, and nothing else"
    );
    assert_eq!(held.weight_bytes, 2 * weight);
    assert_eq!((held.by_key.len(), held.by_last_use.len()), (2, 2));

    // The late build used the entry as a hit does, so the idle one is evicted first.
    let (_used, released) = held.keep(next, compiled(), weight, budget);
    assert!(matches!(&released[..], [only] if same(only, &other)));
    assert!(held.by_key.contains_key(&contended));
    assert!(!held.by_key.contains_key(&idle));
}
