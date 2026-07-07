//! Infers manifest types for SQL placeholders by collecting evidence and
//! resolving it per parameter.
//!
//! Three independent sources contribute claims about a placeholder's type:
//! the types `DataFusion`'s planner writes onto placeholders during SQL
//! planning (comparisons, `BETWEEN`, `IN`, `LIKE`), explicit `CAST` /
//! `TRY_CAST` targets, and declared source table-function argument types.
//! Collection never merges: an untyped occurrence is simply no evidence.
//! [`resolve`] then decides per parameter: declared claims outrank Arrow
//! claims, disagreements are errors carrying every claim with its
//! provenance, and a parameter with no claims at all is ambiguous.
//! Arrow-to-manifest lowering is lossy by design — see [`crate::types`] for
//! the policies and their invariants.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use arrow::datatypes::DataType;
use coral_spec::ManifestDataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::runtime::source_functions::SourceFunctionNode;
use crate::types::{manifest_claim_accepts_arrow, manifest_data_type_for_arrow};

/// One SQL parameter with its resolved manifest type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InferredParameter {
    pub(crate) name: String,
    pub(crate) data_type: ManifestDataType,
}

/// Why a parameter's type could not be resolved.
#[derive(Debug)]
pub(crate) enum ParameterInferenceError {
    /// No source produced a claim for the parameter.
    NoEvidence { parameter: String },
    /// Claims disagree; every claim is included so the author can see both
    /// sides of the disagreement.
    Conflict {
        parameter: String,
        claims: Vec<TypeEvidence>,
    },
    /// The claims agree, but on a type the manifest vocabulary cannot express.
    Inexpressible {
        parameter: String,
        data_type: DataType,
        source: EvidenceSource,
    },
}

/// One claim about one parameter's type, tagged with where it came from.
#[derive(Debug, Clone)]
pub(crate) struct TypeEvidence {
    parameter: String,
    claim: TypeClaim,
    source: EvidenceSource,
}

/// What a claim says the type is, in whichever vocabulary the source speaks.
#[derive(Debug, Clone)]
enum TypeClaim {
    /// A manifest type stated by metadata; carries more information than any
    /// Arrow spelling (`Json` has no Arrow spelling of its own).
    Declared(ManifestDataType),
    /// An Arrow type observed in the plan.
    Arrow(DataType),
}

/// Where a claim came from; shown verbatim in conflict errors.
#[derive(Debug, Clone)]
pub(crate) enum EvidenceSource {
    Planner,
    CastTarget,
    SourceFunctionArgument { function: String, argument: String },
}

/// Infers the manifest type of every SQL placeholder in `plan`.
pub(crate) fn infer_parameters(plan: &LogicalPlan) -> Result<Vec<InferredParameter>> {
    let parameters = placeholder_names(plan)?;

    let mut evidence = planner_evidence(plan)?;
    evidence.extend(cast_evidence(plan)?);
    evidence.extend(source_function_evidence(plan)?);

    resolve(parameters, evidence).map_err(|error| DataFusionError::Plan(error.to_string()))
}

/// Every placeholder referenced by the plan, typed or not.
fn placeholder_names(plan: &LogicalPlan) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for_each_expr(plan, |expr| {
        if let Expr::Placeholder(placeholder) = expr {
            names.insert(placeholder.id.clone());
        }
        Ok(())
    })?;
    Ok(names)
}

/// Claims from types the planner already wrote onto placeholders.
///
/// Read directly off the plan rather than via
/// [`LogicalPlan::get_parameter_fields`], which folds occurrences with its
/// own merge semantics: an untyped occurrence can erase an earlier typed
/// one, and conflicting occurrences error there without provenance. Merging
/// is [`resolve`]'s job alone.
fn planner_evidence(plan: &LogicalPlan) -> Result<Vec<TypeEvidence>> {
    let mut evidence = Vec::new();
    for_each_expr(plan, |expr| {
        let Expr::Placeholder(placeholder) = expr else {
            return Ok(());
        };
        let Some(field) = &placeholder.field else {
            return Ok(());
        };
        evidence.push(TypeEvidence {
            parameter: placeholder.id.clone(),
            claim: TypeClaim::Arrow(field.data_type().clone()),
            source: EvidenceSource::Planner,
        });
        Ok(())
    })?;
    Ok(evidence)
}

/// Claims from explicit `CAST` / `TRY_CAST` targets over a placeholder.
fn cast_evidence(plan: &LogicalPlan) -> Result<Vec<TypeEvidence>> {
    let mut evidence = Vec::new();
    for_each_expr(plan, |expr| {
        let (cast_input, target) = match expr {
            Expr::Cast(cast) => (cast.expr.as_ref(), &cast.data_type),
            Expr::TryCast(cast) => (cast.expr.as_ref(), &cast.data_type),
            _ => return Ok(()),
        };
        let Expr::Placeholder(placeholder) = cast_input else {
            return Ok(());
        };
        evidence.push(TypeEvidence {
            parameter: placeholder.id.clone(),
            claim: TypeClaim::Arrow(target.clone()),
            source: EvidenceSource::CastTarget,
        });
        Ok(())
    })?;
    Ok(evidence)
}

/// Claims from declared source table-function argument types, for
/// placeholders passed directly as argument values.
fn source_function_evidence(plan: &LogicalPlan) -> Result<Vec<TypeEvidence>> {
    let mut evidence = Vec::new();
    plan.apply_with_subqueries(|node| {
        let LogicalPlan::Extension(extension) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(function) = extension.node.as_any().downcast_ref::<SourceFunctionNode>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        for (argument, expr) in function.declared_args_with_call_exprs() {
            let Expr::Placeholder(placeholder) = expr else {
                continue;
            };
            evidence.push(TypeEvidence {
                parameter: placeholder.id.clone(),
                claim: TypeClaim::Declared(argument.data_type),
                source: EvidenceSource::SourceFunctionArgument {
                    function: function.display_name().to_string(),
                    argument: argument.name.clone(),
                },
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(evidence)
}

/// Visits every expression in the plan, including subqueries.
fn for_each_expr(plan: &LogicalPlan, mut visit: impl FnMut(&Expr) -> Result<()>) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                visit(expr)?;
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(())
}

/// Resolves each parameter's claims to one manifest type.
fn resolve(
    parameters: BTreeSet<String>,
    evidence: Vec<TypeEvidence>,
) -> std::result::Result<Vec<InferredParameter>, ParameterInferenceError> {
    let mut claims_by_parameter: BTreeMap<String, Vec<TypeEvidence>> = BTreeMap::new();
    for item in evidence {
        claims_by_parameter
            .entry(item.parameter.clone())
            .or_default()
            .push(item);
    }

    let mut inferred = Vec::new();
    for parameter in parameters {
        let claims = claims_by_parameter.remove(&parameter).unwrap_or_default();
        let data_type = resolve_parameter(&parameter, &claims)?;
        inferred.push(InferredParameter {
            name: parameter,
            data_type,
        });
    }
    Ok(inferred)
}

/// Resolves one parameter's claims by precedence: a declared claim wins and
/// every Arrow claim must be a spelling it accepts; with no declaration, all
/// Arrow claims must resolve to the same manifest type; with no claims at
/// all, the parameter is ambiguous.
fn resolve_parameter(
    parameter: &str,
    claims: &[TypeEvidence],
) -> std::result::Result<ManifestDataType, ParameterInferenceError> {
    if let Some(declared) = agreed_declared_type(parameter, claims)? {
        ensure_arrow_claims_accept(parameter, declared, claims)?;
        return Ok(declared);
    }

    match agreed_arrow_type(parameter, claims)? {
        Some(resolved) => Ok(resolved),
        None => Err(ParameterInferenceError::NoEvidence {
            parameter: parameter.to_string(),
        }),
    }
}

/// The single declared type, if any declared claims exist.
fn agreed_declared_type(
    parameter: &str,
    claims: &[TypeEvidence],
) -> std::result::Result<Option<ManifestDataType>, ParameterInferenceError> {
    let mut agreed = None;
    for item in claims {
        let TypeClaim::Declared(declared) = item.claim else {
            continue;
        };
        match agreed {
            None => agreed = Some(declared),
            Some(existing) if existing == declared => {}
            Some(_) => return Err(conflict(parameter, claims)),
        }
    }
    Ok(agreed)
}

/// Checks that every Arrow claim is a spelling the declared type accepts
/// (see [`manifest_claim_accepts_arrow`]).
fn ensure_arrow_claims_accept(
    parameter: &str,
    declared: ManifestDataType,
    claims: &[TypeEvidence],
) -> std::result::Result<(), ParameterInferenceError> {
    for item in claims {
        let TypeClaim::Arrow(ref arrow) = item.claim else {
            continue;
        };
        if !manifest_claim_accepts_arrow(declared, arrow) {
            return Err(conflict(parameter, claims));
        }
    }
    Ok(())
}

/// The single manifest type all Arrow claims resolve to, if any exist.
///
/// Each claim resolves to manifest spelling before agreement is checked, so
/// Arrow spelling differences (`Utf8` vs `Utf8View`, timestamp units) cannot
/// manufacture conflicts.
fn agreed_arrow_type(
    parameter: &str,
    claims: &[TypeEvidence],
) -> std::result::Result<Option<ManifestDataType>, ParameterInferenceError> {
    let mut agreed = None;
    for item in claims {
        let TypeClaim::Arrow(ref arrow) = item.claim else {
            continue;
        };
        let Some(resolved) = manifest_data_type_for_arrow(arrow) else {
            return Err(ParameterInferenceError::Inexpressible {
                parameter: parameter.to_string(),
                data_type: arrow.clone(),
                source: item.source.clone(),
            });
        };
        match agreed {
            None => agreed = Some(resolved),
            Some(existing) if existing == resolved => {}
            Some(_) => return Err(conflict(parameter, claims)),
        }
    }
    Ok(agreed)
}

fn conflict(parameter: &str, claims: &[TypeEvidence]) -> ParameterInferenceError {
    ParameterInferenceError::Conflict {
        parameter: parameter.to_string(),
        claims: claims.to_vec(),
    }
}

impl fmt::Display for ParameterInferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoEvidence { parameter } => write!(
                f,
                "SQL parameter '{parameter}' has no inferred type; cast it in SQL, for example CAST({parameter} AS VARCHAR)"
            ),
            Self::Conflict { parameter, claims } => {
                write!(
                    f,
                    "SQL parameter '{parameter}' has conflicting type evidence: "
                )?;
                for (index, claim) in claims.iter().enumerate() {
                    if index > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "{claim}")?;
                }
                Ok(())
            }
            Self::Inexpressible {
                parameter,
                data_type,
                source,
            } => write!(
                f,
                "SQL parameter '{parameter}' inferred unsupported type {data_type} from {source}; supported function argument types are Utf8, Int64, Float64, Boolean, Timestamp, and Json"
            ),
        }
    }
}

impl fmt::Display for TypeEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} from {}", self.claim, self.source)
    }
}

impl fmt::Display for TypeClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Declared(data_type) => write!(f, "declared {}", data_type.as_manifest_str()),
            Self::Arrow(data_type) => write!(f, "inferred {data_type}"),
        }
    }
}

impl fmt::Display for EvidenceSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planner => write!(f, "DataFusion planning"),
            Self::CastTarget => write!(f, "explicit cast"),
            Self::SourceFunctionArgument { function, argument } => {
                write!(f, "source table function {function} argument '{argument}'")
            }
        }
    }
}
