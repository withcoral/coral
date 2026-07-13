//! Transport-neutral column statistics contracts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const STATISTICS_PROFILE_VERSION: u32 = 1;

/// Persistable statistics profile passed from the app layer into one runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsProfile {
    /// Storage format version.
    pub version: u32,
    /// Source statistics keyed by visible SQL schema name.
    #[serde(default)]
    pub sources: BTreeMap<String, SourceStatistics>,
}

impl Default for StatisticsProfile {
    fn default() -> Self {
        Self {
            version: STATISTICS_PROFILE_VERSION,
            sources: BTreeMap::new(),
        }
    }
}

impl StatisticsProfile {
    /// Returns an empty profile with the current persisted version.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Removes all source entries except the selected schema names.
    pub fn retain_sources<'a>(&mut self, schema_names: impl IntoIterator<Item = &'a str>) {
        let keep = schema_names
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        self.sources
            .retain(|schema_name, _| keep.contains(schema_name.as_str()));
    }
}

/// Statistics known for one selected source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatistics {
    /// Visible SQL schema name.
    pub schema_name: String,
    /// Optional manifest/source version that produced the stats.
    #[serde(default)]
    pub source_version: Option<String>,
    /// Table statistics keyed by visible table name.
    #[serde(default)]
    pub tables: BTreeMap<String, TableStatistics>,
}

/// Statistics known for one source table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Visible SQL schema name.
    pub schema_name: String,
    /// Visible table name.
    pub table_name: String,
    /// Optional manifest/source version that produced the stats.
    #[serde(default)]
    pub source_version: Option<String>,
    /// Schema signature used to reject stale column statistics.
    pub schema_signature: TableSchemaSignature,
    /// Column statistics keyed by column name.
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnStatistics>,
}

/// Stable signature for a table's query-visible shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaSignature {
    /// Columns in ordinal order.
    pub columns: Vec<ColumnSchemaSignature>,
    /// Required filter columns in registered order.
    pub required_filters: Vec<String>,
}

/// Stable signature for one column's query-visible metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchemaSignature {
    /// Column name.
    pub name: String,
    /// Arrow/DataFusion display type.
    pub data_type: String,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Whether the column is virtual.
    pub is_virtual: bool,
    /// Whether the column is a required filter.
    pub is_required_filter: bool,
}

/// Persistable statistics for one column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name.
    pub column_name: String,
    /// Number of rows represented by these stats.
    pub sample_count: u64,
    /// Null count, when known.
    #[serde(default)]
    pub null_count: Option<StatisticValue<u64>>,
    /// Approximate distinct value count, when known.
    #[serde(default)]
    pub approx_distinct_count: Option<StatisticValue<u64>>,
    /// RFC3339 UTC observation timestamp.
    #[serde(default)]
    pub observed_at: Option<String>,
}

impl ColumnStatistics {
    /// Projects `null_count / sample_count` for `coral.columns`.
    #[must_use]
    pub fn null_fraction(&self) -> Option<f64> {
        let null_count = self.null_count.as_ref()?.value;
        if self.sample_count == 0 {
            return None;
        }
        if null_count > self.sample_count {
            return None;
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "coral.columns exposes null_fraction as SQL DOUBLE, so lossy u64-to-f64 projection is intentional."
        )]
        Some(null_count as f64 / self.sample_count as f64)
    }

    /// Projects sample count as SQL `BIGINT`.
    #[must_use]
    pub fn sample_count_i64(&self) -> Option<i64> {
        Some(i64::try_from(self.sample_count).unwrap_or(i64::MAX))
    }

    /// Projects approximate distinct count as SQL `BIGINT`.
    #[must_use]
    pub fn approx_distinct_count_i64(&self) -> Option<i64> {
        self.approx_distinct_count
            .as_ref()
            .and_then(|value| i64::try_from(value.value).ok())
    }

    /// Projects the weakest precision among the displayed statistics.
    #[must_use]
    pub fn precision_for_catalog(&self) -> Option<&'static str> {
        let precision = [
            self.null_count.as_ref(),
            self.approx_distinct_count.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(|value| value.precision)
        .reduce(StatisticPrecision::weaker)
        .or_else(|| (self.sample_count > 0).then_some(StatisticPrecision::Unknown))?;
        Some(precision.as_str())
    }
}

/// A statistic value and its precision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticValue<T> {
    /// Statistic value.
    pub value: T,
    /// Precision of the value.
    pub precision: StatisticPrecision,
}

/// Precision tags exposed through `stats_precision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticPrecision {
    /// Exact for the table snapshot observed.
    Exact,
    /// Approximate, but intentionally computed as an estimate.
    Approximate,
    /// Observed from the rows returned by a scan.
    ObservedSample,
    /// Unknown or mixed precision.
    Unknown,
}

impl StatisticPrecision {
    /// Stable catalog string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Approximate => "approximate",
            Self::ObservedSample => "observed_sample",
            Self::Unknown => "unknown",
        }
    }

    /// Returns the weaker precision, where exact is strongest and unknown is weakest.
    #[must_use]
    pub fn weaker(self, other: Self) -> Self {
        if self.rank() <= other.rank() {
            self
        } else {
            other
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::ObservedSample => 1,
            Self::Approximate => 2,
            Self::Exact => 3,
        }
    }
}

/// One runtime scan observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsObservation {
    /// Visible SQL schema name.
    pub schema_name: String,
    /// Visible table name.
    pub table_name: String,
    /// Optional manifest/source version that produced the observation.
    pub source_version: Option<String>,
    /// Schema signature at observation time.
    pub schema_signature: TableSchemaSignature,
    /// Observation scope.
    pub scope: StatisticsObservationScope,
    /// RFC3339 UTC observation timestamp.
    pub observed_at: String,
    /// Per-column scan observations.
    pub columns: Vec<ColumnStatisticsObservation>,
}

/// Scope for one runtime observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsObservationScope {
    /// The scan represented the table as a whole.
    TableGlobal,
    /// The scan had pushed filters.
    Filtered {
        /// Names of pushed filter columns.
        filter_columns: Vec<String>,
    },
    /// The scan had a pushed limit.
    Limited,
    /// The backend could not defensibly classify scope.
    Unknown,
}

/// Per-column values from one scan observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatisticsObservation {
    /// Column name.
    pub column_name: String,
    /// Number of rows observed.
    pub sample_count: u64,
    /// Null count, when known.
    pub null_count: Option<StatisticValue<u64>>,
    /// Approximate distinct count, when known.
    pub approx_distinct_count: Option<StatisticValue<u64>>,
}

#[cfg(test)]
mod tests {
    use super::{ColumnStatistics, StatisticPrecision, StatisticValue, StatisticsProfile};

    #[test]
    fn empty_profile_uses_current_version() {
        let profile = StatisticsProfile::empty();
        assert_eq!(profile.version, 1);
        assert!(profile.sources.is_empty());
    }

    #[test]
    fn null_fraction_requires_sample_count_and_null_count() {
        let without_nulls = ColumnStatistics {
            column_name: "name".to_string(),
            sample_count: 10,
            null_count: None,
            approx_distinct_count: None,
            observed_at: None,
        };
        assert_eq!(without_nulls.null_fraction(), None);

        let zero_sample = ColumnStatistics {
            null_count: Some(StatisticValue {
                value: 0,
                precision: StatisticPrecision::Exact,
            }),
            sample_count: 0,
            ..without_nulls
        };
        assert_eq!(zero_sample.null_fraction(), None);

        let invalid_count = ColumnStatistics {
            null_count: Some(StatisticValue {
                value: 2,
                precision: StatisticPrecision::Exact,
            }),
            sample_count: 1,
            ..zero_sample
        };
        assert_eq!(invalid_count.null_fraction(), None);
    }

    #[test]
    fn projects_catalog_values() {
        let stats = ColumnStatistics {
            column_name: "name".to_string(),
            sample_count: 10,
            null_count: Some(StatisticValue {
                value: 2,
                precision: StatisticPrecision::Exact,
            }),
            approx_distinct_count: Some(StatisticValue {
                value: 7,
                precision: StatisticPrecision::ObservedSample,
            }),
            observed_at: Some("2026-05-06T00:00:00Z".to_string()),
        };

        assert_eq!(stats.null_fraction(), Some(0.2));
        assert_eq!(stats.sample_count_i64(), Some(10));
        assert_eq!(stats.approx_distinct_count_i64(), Some(7));
        assert_eq!(stats.precision_for_catalog(), Some("observed_sample"));
    }

    #[test]
    fn sample_count_projection_saturates_bigint_overflow() {
        let stats = ColumnStatistics {
            column_name: "name".to_string(),
            sample_count: u64::MAX,
            null_count: None,
            approx_distinct_count: None,
            observed_at: None,
        };

        assert_eq!(stats.sample_count_i64(), Some(i64::MAX));
        assert_eq!(stats.precision_for_catalog(), Some("unknown"));
    }

    #[test]
    fn weaker_precision_keeps_less_defensible_value() {
        assert_eq!(
            StatisticPrecision::Exact.weaker(StatisticPrecision::Approximate),
            StatisticPrecision::Approximate
        );
        assert_eq!(
            StatisticPrecision::ObservedSample.weaker(StatisticPrecision::Unknown),
            StatisticPrecision::Unknown
        );
    }
}
