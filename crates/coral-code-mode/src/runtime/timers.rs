use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::RuntimeCommand;
use super::RuntimeState;
use super::module_loader::is_exit_exception;
use super::value::value_to_error_text;

const MAX_PENDING_TIMEOUTS: usize = 64;
const MAX_TIMEOUT_DELAY_MS: u64 = 60_000;

pub(super) struct ScheduledTimeout {
    callback: v8::Global<v8::Function>,
    cancel_tx: mpsc::Sender<()>,
}

impl Drop for ScheduledTimeout {
    fn drop(&mut self) {
        let _ = self.cancel_tx.send(());
    }
}

pub(super) fn schedule_timeout(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<u64, String> {
    let callback = args.get(0);
    if !callback.is_function() {
        return Err("setTimeout expects a function callback".to_string());
    }
    let callback = v8::Local::<v8::Function>::try_from(callback)
        .map_err(|_| "setTimeout expects a function callback".to_string())?;

    let delay_ms = args
        .get(1)
        .number_value(scope)
        .map(normalize_delay_ms)
        .unwrap_or(Ok(0))?;

    let callback = v8::Global::new(scope, callback);
    let state = scope
        .get_slot_mut::<RuntimeState>()
        .ok_or_else(|| "runtime state unavailable".to_string())?;
    if state.pending_timeouts.len() >= MAX_PENDING_TIMEOUTS {
        return Err(format!(
            "setTimeout supports at most {MAX_PENDING_TIMEOUTS} pending timers per Code Mode cell"
        ));
    }
    let timeout_id = state.next_timeout_id;
    state.next_timeout_id = state.next_timeout_id.saturating_add(1);
    let runtime_command_tx = state.runtime_command_tx.clone();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    state.pending_timeouts.insert(
        timeout_id,
        ScheduledTimeout {
            callback,
            cancel_tx,
        },
    );
    thread::spawn(move || {
        if cancel_rx
            .recv_timeout(Duration::from_millis(delay_ms))
            .is_err()
        {
            let _ = runtime_command_tx.send(RuntimeCommand::TimeoutFired { id: timeout_id });
        }
    });

    Ok(timeout_id)
}

pub(super) fn clear_timeout(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<(), String> {
    let Some(timeout_id) = timeout_id_from_args(scope, args)? else {
        return Ok(());
    };

    let Some(state) = scope.get_slot_mut::<RuntimeState>() else {
        return Err("runtime state unavailable".to_string());
    };
    state.pending_timeouts.remove(&timeout_id);
    Ok(())
}

pub(super) fn invoke_timeout_callback(
    scope: &mut v8::PinScope<'_, '_>,
    timeout_id: u64,
) -> Result<(), String> {
    let callback = {
        let state = scope
            .get_slot_mut::<RuntimeState>()
            .ok_or_else(|| "runtime state unavailable".to_string())?;
        state.pending_timeouts.remove(&timeout_id)
    };
    let Some(callback) = callback else {
        return Ok(());
    };

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let callback = v8::Local::new(&tc, &callback.callback);
    let receiver = v8::undefined(&tc).into();
    let _ = callback.call(&tc, receiver, &[]);
    if tc.has_caught() {
        if let Some(exception) = tc.exception()
            && is_exit_exception(&mut tc, exception)
        {
            return Ok(());
        }
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| "unknown code mode exception".to_string()));
    }

    Ok(())
}
fn timeout_id_from_args(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
) -> Result<Option<u64>, String> {
    if args.length() == 0 || args.get(0).is_null_or_undefined() {
        return Ok(None);
    }

    let Some(timeout_id) = args.get(0).number_value(scope) else {
        return Err("clearTimeout expects a numeric timeout id".to_string());
    };
    if !timeout_id.is_finite() || timeout_id <= 0.0 {
        return Ok(None);
    }

    Ok(Some(timeout_id.trunc().min(u64::MAX as f64) as u64))
}

fn normalize_delay_ms(delay_ms: f64) -> Result<u64, String> {
    if !delay_ms.is_finite() || delay_ms <= 0.0 {
        return Ok(0);
    }
    let delay_ms = delay_ms.trunc().min(u64::MAX as f64) as u64;
    if delay_ms > MAX_TIMEOUT_DELAY_MS {
        return Err(format!(
            "setTimeout delay must be at most {MAX_TIMEOUT_DELAY_MS} ms"
        ));
    }
    Ok(delay_ms)
}

#[cfg(test)]
mod tests {
    use super::MAX_TIMEOUT_DELAY_MS;
    use super::normalize_delay_ms;

    #[test]
    fn normalize_delay_rejects_long_timers() {
        normalize_delay_ms((MAX_TIMEOUT_DELAY_MS + 1) as f64)
            .expect_err("delay above the maximum should be rejected");
    }

    #[test]
    fn normalize_delay_allows_zero_and_short_timers() {
        assert_eq!(normalize_delay_ms(f64::NAN).unwrap(), 0);
        assert_eq!(normalize_delay_ms(-1.0).unwrap(), 0);
        assert_eq!(normalize_delay_ms(12.9).unwrap(), 12);
    }
}
