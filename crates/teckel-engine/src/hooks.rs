//! Lifecycle hooks execution (Section 16).
//!
//! Pre-execution hooks run sequentially before any asset executes.
//! If any pre-hook exits non-zero, the pipeline MUST NOT start (E-HOOK-001).
//! Post-execution hooks run after pipeline completes (success or failure).
//! Post-hook failures are logged but don't change pipeline status.

use teckel_model::pipeline::Hooks;
use teckel_model::{TeckelError, TeckelErrorCode};
use tokio::process::Command;

/// Execute pre-execution hooks. Returns error if any hook fails.
pub async fn run_pre_hooks(hooks: &Hooks, pipeline_file: Option<&str>) -> Result<(), TeckelError> {
    for hook in &hooks.pre_execution {
        tracing::info!(hook = %hook.name, "running pre-execution hook");
        let status = run_hook_command(&hook.command, "pre", pipeline_file).await?;
        if !status.success() {
            let code = status.code().unwrap_or(-1);
            return Err(TeckelError::spec(
                TeckelErrorCode::EHook001,
                format!(
                    "pre-execution hook \"{}\" failed with exit code {code}",
                    hook.name
                ),
            ));
        }
        tracing::debug!(hook = %hook.name, "pre-execution hook completed");
    }
    Ok(())
}

/// Execute post-execution hooks. Failures are logged but not propagated.
pub async fn run_post_hooks(hooks: &Hooks, status: &str, pipeline_file: Option<&str>) {
    for hook in &hooks.post_execution {
        tracing::info!(hook = %hook.name, "running post-execution hook");
        match run_hook_command_with_status(&hook.command, status, pipeline_file).await {
            Ok(exit) => {
                if !exit.success() {
                    tracing::warn!(
                        hook = %hook.name,
                        code = exit.code().unwrap_or(-1),
                        "post-execution hook failed (non-fatal)"
                    );
                } else {
                    tracing::debug!(hook = %hook.name, "post-execution hook completed");
                }
            }
            Err(e) => {
                tracing::warn!(
                    hook = %hook.name,
                    error = %e,
                    "post-execution hook failed to start (non-fatal)"
                );
            }
        }
    }
}

async fn run_hook_command(
    command: &str,
    phase: &str,
    pipeline_file: Option<&str>,
) -> Result<std::process::ExitStatus, TeckelError> {
    run_hook_command_with_status(command, phase, pipeline_file)
        .await
        .map_err(|e| {
            TeckelError::spec(
                TeckelErrorCode::EHook001,
                format!("failed to execute hook command: {e}"),
            )
        })
}

async fn run_hook_command_with_status(
    command: &str,
    status: &str,
    pipeline_file: Option<&str>,
) -> Result<std::process::ExitStatus, std::io::Error> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(command);
    cmd.env("TECKEL_PIPELINE_STATUS", status);
    if let Some(file) = pipeline_file {
        cmd.env("TECKEL_PIPELINE_FILE", file);
    }
    cmd.status().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use teckel_model::pipeline::Hook;

    #[tokio::test]
    async fn pre_hook_success() {
        let hooks = Hooks {
            pre_execution: vec![Hook {
                name: "echo-test".to_string(),
                command: "true".to_string(),
            }],
            post_execution: vec![],
        };
        assert!(run_pre_hooks(&hooks, None).await.is_ok());
    }

    #[tokio::test]
    async fn pre_hook_failure_aborts() {
        let hooks = Hooks {
            pre_execution: vec![Hook {
                name: "fail-hook".to_string(),
                command: "false".to_string(),
            }],
            post_execution: vec![],
        };
        let err = run_pre_hooks(&hooks, None).await.unwrap_err();
        assert!(err.to_string().contains("E-HOOK-001"));
    }

    #[tokio::test]
    async fn post_hook_failure_is_non_fatal() {
        let hooks = Hooks {
            pre_execution: vec![],
            post_execution: vec![Hook {
                name: "fail-post".to_string(),
                command: "false".to_string(),
            }],
        };
        // Should not panic or return error
        run_post_hooks(&hooks, "completed", None).await;
    }
}
