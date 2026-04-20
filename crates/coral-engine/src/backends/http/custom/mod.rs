//! Custom authenticators — compiled-in implementations dispatched from
//! [`AuthSpec::CustomAuth`](coral_spec::AuthSpec). Each submodule implements
//! the [`Authenticator`](super::auth::Authenticator) trait for one
//! [`CustomAuthSpec`](coral_spec::CustomAuthSpec) variant.

pub(crate) mod aws;
