//! Page requests for tests of a list that its owner pages by key.
use std::fmt::Debug;

use coral_db::{Page, PageRequest, PageSize};

/// The request for the page of at most `size` items after the key `after`,
/// or for the first page.
///
/// # Panics
/// Panics when `size` is zero.
#[must_use]
pub fn page_request<K>(after: Option<K>, size: u32) -> PageRequest<K> {
    PageRequest {
        after,
        size: PageSize::new(size).expect("a page of one item or more"),
    }
}

/// Reads a list to its end with `read`, in pages of at most `size` items
/// that each follow the one before, and returns the items of each page.
///
/// # Panics
/// Panics when a read fails, when `size` is zero, or when the list does not
/// end within a thousand pages, as when a cursor does not advance.
pub async fn read_pages<T, K, E>(
    size: u32,
    read: impl AsyncFn(PageRequest<K>) -> Result<Page<T, K>, E>,
) -> Vec<Vec<T>>
where
    E: Debug,
{
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = read(page_request(after, size)).await.expect("read a page");
        pages.push(page.items);
        assert!(pages.len() <= 1000, "the list does not end");
        after = page.next_after;
        if after.is_none() {
            return pages;
        }
    }
}
