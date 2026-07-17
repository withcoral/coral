//! File grouping helpers for file-backed table scans.

use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use object_store::ObjectMeta;

use super::metadata::FileMetadataColumns;
use super::partitions::{
    PartitionColumns, partition_filter_constraints, partition_values_for_path,
};

pub(super) struct FileGroupsForScan {
    pub(super) groups: Vec<FileGroup>,
    pub(super) grouped_by_partition: bool,
}

pub(super) fn file_groups_for_scan(
    table_path: &ListingTableUrl,
    partition_columns: &PartitionColumns,
    files: Vec<ObjectMeta>,
    file_metadata_columns: &FileMetadataColumns,
    filters: &[Expr],
    target_partitions: usize,
    preserve_file_partitions: usize,
) -> Result<FileGroupsForScan> {
    let constraints = partition_filter_constraints(filters, partition_columns);
    let mut partitioned_files = Vec::new();

    for meta in files {
        let partition_values =
            partition_values_for_path(table_path, &meta.location, partition_columns)?;
        if !constraints.matches(&partition_values) {
            continue;
        }

        let mut partition_values = partition_values.into_scalars();
        partition_values.extend(file_metadata_columns.file_values(table_path, &meta.location)?);
        partitioned_files
            .push(PartitionedFile::new_from_meta(meta).with_partition_values(partition_values));
    }

    if partitioned_files.is_empty() {
        return Ok(FileGroupsForScan {
            groups: vec![FileGroup::default()],
            grouped_by_partition: false,
        });
    }

    partitioned_files.sort_by(|left, right| {
        left.object_meta
            .location
            .as_ref()
            .cmp(right.object_meta.location.as_ref())
    });

    let file_group = FileGroup::new(partitioned_files);
    let target_partitions = target_partitions.max(1);
    if partition_columns.is_empty()
        || file_metadata_columns.has_file_columns()
        || preserve_file_partitions == 0
    {
        return Ok(FileGroupsForScan {
            groups: file_group.split_files(target_partitions),
            grouped_by_partition: false,
        });
    }

    let grouped = file_group.group_by_partition_values(target_partitions);
    if grouped.len() >= preserve_file_partitions {
        Ok(FileGroupsForScan {
            groups: grouped,
            grouped_by_partition: true,
        })
    } else {
        let files = grouped
            .into_iter()
            .flat_map(FileGroup::into_inner)
            .collect::<Vec<_>>();
        Ok(FileGroupsForScan {
            groups: FileGroup::new(files).split_files(target_partitions),
            grouped_by_partition: false,
        })
    }
}
