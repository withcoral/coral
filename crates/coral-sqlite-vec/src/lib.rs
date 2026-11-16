//! sqlite-vec inside Coral's one `SQLite`.
//!
//! Universal Search keeps its `SQLite` vectors in a sqlite-vec `vec0` table
//! (ADR 0035, "Storage"). The `sqlite-vec` crate compiles the extension into
//! the binary against the bundled `SQLite` that sqlx and rusqlite share.
//! [`register`] makes `SQLite` load it into every connection the process opens
//! after the call, whichever library opens it. coral-db calls it before it
//! opens a pool.
//!
//! This crate holds the one `unsafe` block of Coral's own code: the call that
//! hands the extension's entry point to `SQLite`. Its manifest copies the
//! workspace lints with `unsafe_code` at `deny` instead of `forbid`.

use std::ffi::{c_char, c_int};
use std::sync::OnceLock;

use libsqlite3_sys::{SQLITE_OK, sqlite3, sqlite3_api_routines};

/// The entry point `SQLite` calls on each connection it opens: the type
/// `sqlite3_auto_extension` takes.
type EntryPoint =
    unsafe extern "C" fn(*mut sqlite3, *mut *mut c_char, *const sqlite3_api_routines) -> c_int;

/// `SQLite` refused to add sqlite-vec to the extensions it loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SQLite refused to load sqlite-vec into new connections (result code {code})")]
pub struct RegisterError {
    /// `SQLite`'s result code.
    pub code: i32,
}

/// Makes `SQLite` load sqlite-vec into every connection the process opens
/// from now on. Connections already open do not get it. The first call
/// registers the extension; every later call returns its result.
///
/// # Errors
///
/// Returns [`RegisterError`] when `SQLite` cannot add the extension, which
/// it reports only when it runs out of memory.
pub fn register() -> Result<(), RegisterError> {
    static REGISTERED: OnceLock<c_int> = OnceLock::new();
    let code = *REGISTERED.get_or_init(|| {
        #[expect(
            unsafe_code,
            reason = "SQLite takes a statically linked extension only through this C call"
        )]
        // SAFETY: the `sqlite-vec` crate declares `sqlite3_vec_init` with no
        // arguments to match SQLite's `void (*)(void)` prototype for
        // `sqlite3_auto_extension`; its C definition is
        // `int sqlite3_vec_init(sqlite3 *, char **, const sqlite3_api_routines *)`,
        // the entry point type SQLite calls it with. Both are function
        // pointers of one size. `sqlite3_auto_extension` is thread safe (it
        // holds SQLite's main mutex) and ignores an entry point it already
        // has. The extension is compiled with `SQLITE_CORE`, so it calls the
        // bundled SQLite's functions directly and never reads the API table.
        unsafe {
            let entry_point = std::mem::transmute::<unsafe extern "C" fn(), EntryPoint>(
                sqlite_vec::sqlite3_vec_init,
            );
            libsqlite3_sys::sqlite3_auto_extension(Some(entry_point))
        }
    });
    if code == SQLITE_OK {
        Ok(())
    } else {
        Err(RegisterError { code })
    }
}
