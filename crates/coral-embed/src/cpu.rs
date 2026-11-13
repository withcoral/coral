//! The CPU floor: below x86-64-v3 the vector side stays off.

/// The CPU lacks features of x86-64-v3, so Coral loads no model on it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "this CPU is too old for search by meaning: it lacks the x86-64-v3 features {}",
    .missing.join(", ")
)]
pub struct CpuTooOld {
    /// The missing features, by their `is_x86_feature_detected!` names.
    pub missing: Vec<&'static str>,
}

/// Checks that this CPU meets the floor of ADR 0035: x86-64-v3 on x86-64.
/// Every arm64 CPU meets it.
///
/// # Errors
///
/// [`CpuTooOld`] names the x86-64-v3 features this CPU lacks.
pub fn check_cpu() -> Result<(), CpuTooOld> {
    #[cfg(target_arch = "x86_64")]
    {
        check_features(x86_64_v3::detect())
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        Ok(())
    }
}

/// Fails with the features that `detected` marks absent.
#[cfg_attr(
    all(not(target_arch = "x86_64"), not(test)),
    expect(dead_code, reason = "only x86-64 has features to check")
)]
fn check_features(
    detected: impl IntoIterator<Item = (&'static str, bool)>,
) -> Result<(), CpuTooOld> {
    let missing: Vec<_> = detected
        .into_iter()
        .filter(|&(_, present)| !present)
        .map(|(feature, _)| feature)
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(CpuTooOld { missing })
    }
}

#[cfg(target_arch = "x86_64")]
mod x86_64_v3 {
    /// Each feature of the x86-64-v3 level, with whether this CPU has it.
    /// The level includes x86-64-v2, so its features are here too. LAHF/SAHF
    /// is not here: `is_x86_feature_detected!` cannot detect it, and every
    /// CPU that has AVX2, BMI2 and FMA also has it.
    pub(super) fn detect() -> [(&'static str, bool); 15] {
        [
            ("cmpxchg16b", is_x86_feature_detected!("cmpxchg16b")),
            ("popcnt", is_x86_feature_detected!("popcnt")),
            ("sse3", is_x86_feature_detected!("sse3")),
            ("ssse3", is_x86_feature_detected!("ssse3")),
            ("sse4.1", is_x86_feature_detected!("sse4.1")),
            ("sse4.2", is_x86_feature_detected!("sse4.2")),
            ("avx", is_x86_feature_detected!("avx")),
            ("avx2", is_x86_feature_detected!("avx2")),
            ("bmi1", is_x86_feature_detected!("bmi1")),
            ("bmi2", is_x86_feature_detected!("bmi2")),
            ("f16c", is_x86_feature_detected!("f16c")),
            ("fma", is_x86_feature_detected!("fma")),
            ("lzcnt", is_x86_feature_detected!("lzcnt")),
            ("movbe", is_x86_feature_detected!("movbe")),
            ("xsave", is_x86_feature_detected!("xsave")),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cpu_without_an_x86_64_v3_feature_is_too_old() {
        for (detected, expected) in [
            (vec![("avx", true), ("avx2", true), ("fma", true)], Ok(())),
            (
                vec![("avx", true), ("avx2", false), ("fma", false)],
                Err(CpuTooOld {
                    missing: vec!["avx2", "fma"],
                }),
            ),
        ] {
            assert_eq!(check_features(detected.clone()), expected, "{detected:?}");
        }
    }

    #[test]
    fn too_old_names_the_missing_features() {
        let error = CpuTooOld {
            missing: vec!["avx2", "fma"],
        };
        assert_eq!(
            error.to_string(),
            "this CPU is too old for search by meaning: it lacks the x86-64-v3 features avx2, fma"
        );
    }
}
