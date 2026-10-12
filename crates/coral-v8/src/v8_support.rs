use std::sync::OnceLock;

use crate::Error;

static V8_INITIALIZED: OnceLock<Result<(), String>> = OnceLock::new();

pub(crate) fn initialize() -> Result<(), Error> {
    match V8_INITIALIZED.get_or_init(|| {
        // Include tracked external memory in V8's global budget without its
        // default multiplier. This remains a pressure threshold, not a hard cap.
        v8::V8::set_flags_from_string(
            "--enforce-global-heap-limit --maximum-global-heap-limit-factor=1",
        );
        v8::icu::set_common_data_78(deno_core_icudata::ICU_DATA)
            .map_err(|code| format!("failed to initialize ICU data: error {code}"))?;
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
        Ok(())
    }) {
        Ok(()) => Ok(()),
        Err(message) => Err(Error::Initialization(message.clone())),
    }
}

pub(crate) fn run_script(
    scope: &mut v8::PinScope<'_, '_>,
    source_text: &str,
    resource_name: &str,
) -> Result<(), Error> {
    evaluate_script(scope, source_text, resource_name).map(|_value| ())
}

pub(crate) fn evaluate_script<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    source_text: &str,
    resource_name: &str,
) -> Result<v8::Local<'s, v8::Value>, Error> {
    let try_catch = std::pin::pin!(v8::TryCatch::new(scope));
    let mut try_catch = try_catch.init();
    let source = v8::String::new(&try_catch, source_text)
        .ok_or_else(|| Error::Engine(format!("failed to allocate {resource_name}")))?;
    let name = v8::String::new(&try_catch, resource_name)
        .ok_or_else(|| Error::Engine(format!("failed to allocate {resource_name} name")))?;
    let origin = v8::ScriptOrigin::new(
        &try_catch,
        name.into(),
        0,
        0,
        false,
        -1,
        None,
        false,
        false,
        false,
        None,
    );
    let mut source = v8::script_compiler::Source::new(source, Some(&origin));
    let script = v8::script_compiler::compile(
        &try_catch,
        &mut source,
        v8::script_compiler::CompileOptions::NoCompileOptions,
        v8::script_compiler::NoCacheReason::NoReason,
    )
    .ok_or_else(|| match try_catch.exception() {
        Some(exception) => Error::Engine(format_thrown_value(&mut try_catch, exception)),
        None => Error::Engine(format!("failed to compile {resource_name}")),
    })?;
    script
        .run(&try_catch)
        .ok_or_else(|| match try_catch.exception() {
            Some(exception) => Error::Engine(format_thrown_value(&mut try_catch, exception)),
            None => Error::Engine(format!("failed to evaluate {resource_name}")),
        })
}

pub(crate) fn read_global_string(
    scope: &mut v8::PinScope<'_, '_>,
    name: &str,
) -> Result<String, Error> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, name)
        .ok_or_else(|| Error::Engine(format!("failed to allocate global key {name}")))?;
    let value = global
        .get(scope, key.into())
        .ok_or_else(|| Error::Engine(format!("missing global {name}")))?;
    Ok(value.to_rust_string_lossy(scope))
}

pub(crate) fn format_thrown_value(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> String {
    if value.is_object()
        && let Ok(object) = v8::Local::<v8::Object>::try_from(value)
        && let Some(key) = v8::String::new(scope, "stack")
        && let Some(stack) = object.get(scope, key.into())
        && stack.is_string()
    {
        return stack.to_rust_string_lossy(scope);
    }
    value.to_rust_string_lossy(scope)
}
