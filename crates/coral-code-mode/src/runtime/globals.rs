use super::RuntimeState;
use super::callbacks::clear_timeout_callback;
use super::callbacks::console_callback;
use super::callbacks::exit_callback;
use super::callbacks::image_callback;
use super::callbacks::load_callback;
use super::callbacks::set_timeout_callback;
use super::callbacks::store_callback;
use super::callbacks::tool_callback;
use super::callbacks::yield_control_callback;

pub(super) fn install_globals(scope: &mut v8::PinScope<'_, '_>) -> Result<(), String> {
    let global = scope.get_current_context().global(scope);
    let console = build_console_value(scope)?;
    delete_global(scope, global, "Atomics")?;
    delete_global(scope, global, "SharedArrayBuffer")?;
    delete_global(scope, global, "WebAssembly")?;

    let (tools, coral) = build_global_tool_objects(scope)?;
    let all_tools = build_all_tools_value(scope)?;
    let clear_timeout = helper_function(scope, "clearTimeout", clear_timeout_callback)?;
    let set_timeout = helper_function(scope, "setTimeout", set_timeout_callback)?;
    let image = helper_function(scope, "image", image_callback)?;
    let store = helper_function(scope, "store", store_callback)?;
    let load = helper_function(scope, "load", load_callback)?;
    let yield_control = helper_function(scope, "yield_control", yield_control_callback)?;
    let exit = helper_function(scope, "exit", exit_callback)?;

    set_global(scope, global, "console", console.into())?;
    set_global(scope, global, "tools", tools.into())?;
    set_global(scope, global, "coral", coral.into())?;
    set_global(scope, global, "ALL_TOOLS", all_tools)?;
    set_global(scope, global, "clearTimeout", clear_timeout.into())?;
    set_global(scope, global, "setTimeout", set_timeout.into())?;
    set_global(scope, global, "image", image.into())?;
    set_global(scope, global, "store", store.into())?;
    set_global(scope, global, "load", load.into())?;
    set_global(scope, global, "yield_control", yield_control.into())?;
    set_global(scope, global, "exit", exit.into())?;
    Ok(())
}

fn build_console_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Object>, String> {
    let console = v8::Object::new(scope);
    for method in ["log", "info", "warn", "error", "debug"] {
        let function = helper_function(scope, method, console_callback)?;
        set_nested_property(scope, console, &[method], function.into())?;
    }
    Ok(console)
}

fn build_global_tool_objects<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<(v8::Local<'s, v8::Object>, v8::Local<'s, v8::Object>), String> {
    let tools = null_prototype_object(scope)?;
    let coral = null_prototype_object(scope)?;
    let enabled_tools = scope
        .get_slot::<RuntimeState>()
        .map(|state| state.enabled_tools.clone())
        .unwrap_or_default();

    for (tool_index, tool) in enabled_tools.iter().enumerate() {
        let function = tool_function(scope, tool_index)?;
        install_tool_path(scope, tools, coral, &tool.global_name, function)?;
    }
    Ok((tools, coral))
}

fn install_tool_path<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    tools: v8::Local<'s, v8::Object>,
    coral: v8::Local<'s, v8::Object>,
    global_name: &str,
    function: v8::Local<'s, v8::Function>,
) -> Result<(), String> {
    let segments = global_name
        .split('.')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let Some((root, path)) = segments.split_first() else {
        return Err("tool global path must not be empty".to_string());
    };
    match (*root, path) {
        ("tools", path) if !path.is_empty() => {
            set_nested_property(scope, tools, path, function.into())
        }
        ("coral", path) if !path.is_empty() => {
            set_nested_property(scope, coral, path, function.into())
        }
        _ => set_nested_property(scope, tools, &segments, function.into()),
    }
}

fn set_nested_property<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    root: v8::Local<'s, v8::Object>,
    path: &[&str],
    value: v8::Local<'s, v8::Value>,
) -> Result<(), String> {
    let Some((leaf, parents)) = path.split_last() else {
        return Err("tool global path must not be empty".to_string());
    };
    let mut object = root;
    for segment in parents {
        object = ensure_child_object(scope, object, segment)?;
    }
    let key = v8::String::new(scope, leaf)
        .ok_or_else(|| format!("failed to allocate tool path segment `{leaf}`"))?;
    if object.set(scope, key.into(), value) == Some(true) {
        Ok(())
    } else {
        Err(format!(
            "failed to set tool global path `{}`",
            path.join(".")
        ))
    }
}

fn ensure_child_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    parent: v8::Local<'s, v8::Object>,
    segment: &str,
) -> Result<v8::Local<'s, v8::Object>, String> {
    let key = v8::String::new(scope, segment)
        .ok_or_else(|| format!("failed to allocate tool path segment `{segment}`"))?;
    let owns_property = parent
        .has_own_property(scope, key.into())
        .ok_or_else(|| format!("failed to inspect tool path segment `{segment}`"))?;
    if owns_property {
        let existing = parent
            .get(scope, key.into())
            .ok_or_else(|| format!("failed to read tool path segment `{segment}`"))?;
        if existing.is_object() && !existing.is_function() {
            return v8::Local::<v8::Object>::try_from(existing)
                .map_err(|_| format!("tool global path `{segment}` is not an object"));
        }
        return Err(format!(
            "tool global path segment `{segment}` conflicts with an existing value"
        ));
    }
    let child = null_prototype_object(scope)?;
    if parent.set(scope, key.into(), child.into()) == Some(true) {
        Ok(child)
    } else {
        Err(format!(
            "failed to create tool global path segment `{segment}`"
        ))
    }
}

fn null_prototype_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Object>, String> {
    let object = v8::Object::new(scope);
    if object.set_prototype(scope, v8::null(scope).into()) == Some(true) {
        Ok(object)
    } else {
        Err("failed to create null-prototype tool namespace object".to_string())
    }
}

fn build_all_tools_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Value>, String> {
    let enabled_tools = scope
        .get_slot::<RuntimeState>()
        .map(|state| state.enabled_tools.clone())
        .unwrap_or_default();
    let array = v8::Array::new(scope, enabled_tools.len() as i32);
    let name_key = v8::String::new(scope, "name")
        .ok_or_else(|| "failed to allocate ALL_TOOLS name key".to_string())?;
    let description_key = v8::String::new(scope, "description")
        .ok_or_else(|| "failed to allocate ALL_TOOLS description key".to_string())?;

    for (index, tool) in enabled_tools.iter().enumerate() {
        let item = v8::Object::new(scope);
        let name = v8::String::new(scope, &tool.global_name)
            .ok_or_else(|| "failed to allocate ALL_TOOLS name".to_string())?;
        let description = v8::String::new(scope, &tool.description)
            .ok_or_else(|| "failed to allocate ALL_TOOLS description".to_string())?;

        if item.set(scope, name_key.into(), name.into()) != Some(true) {
            return Err("failed to set ALL_TOOLS name".to_string());
        }
        if item.set(scope, description_key.into(), description.into()) != Some(true) {
            return Err("failed to set ALL_TOOLS description".to_string());
        }
        if array.set_index(scope, index as u32, item.into()) != Some(true) {
            return Err("failed to append ALL_TOOLS metadata".to_string());
        }
    }

    Ok(array.into())
}

fn helper_function<'s, F>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
    callback: F,
) -> Result<v8::Local<'s, v8::Function>, String>
where
    F: v8::MapFnTo<v8::FunctionCallback>,
{
    let name =
        v8::String::new(scope, name).ok_or_else(|| "failed to allocate helper name".to_string())?;
    let template = v8::FunctionTemplate::builder(callback)
        .data(name.into())
        .build(scope);
    template
        .get_function(scope)
        .ok_or_else(|| "failed to create helper function".to_string())
}

fn tool_function<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    tool_index: usize,
) -> Result<v8::Local<'s, v8::Function>, String> {
    let data = v8::String::new(scope, &tool_index.to_string())
        .ok_or_else(|| "failed to allocate tool callback data".to_string())?;
    let template = v8::FunctionTemplate::builder(tool_callback)
        .data(data.into())
        .build(scope);
    template
        .get_function(scope)
        .ok_or_else(|| "failed to create tool function".to_string())
}

fn set_global<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    global: v8::Local<'s, v8::Object>,
    name: &str,
    value: v8::Local<'s, v8::Value>,
) -> Result<(), String> {
    let key = v8::String::new(scope, name)
        .ok_or_else(|| format!("failed to allocate global `{name}`"))?;
    if global.set(scope, key.into(), value) == Some(true) {
        Ok(())
    } else {
        Err(format!("failed to set global `{name}`"))
    }
}

fn delete_global<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    global: v8::Local<'s, v8::Object>,
    name: &str,
) -> Result<(), String> {
    let key = v8::String::new(scope, name)
        .ok_or_else(|| format!("failed to allocate global `{name}`"))?;
    if global.delete(scope, key.into()) == Some(true) {
        Ok(())
    } else {
        Err(format!("failed to remove global `{name}`"))
    }
}
