use std::sync::{Arc, Mutex};

use scheme_rs::exceptions::Exception;
use scheme_rs::proc::{Application, ContBarrier, Procedure};
use scheme_rs::registry::{bridge, cps_bridge};
use scheme_rs::runtime::Runtime;
use scheme_rs::value::Value;
use tokio::task::JoinSet;

tokio::task_local! {
    static CURRENT_SCOPE: Arc<Mutex<JoinSet<()>>>;
}

async fn run_tasks_impl(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    let join_set = Arc::new(Mutex::new(JoinSet::new()));
    let js = Arc::clone(&join_set);

    let result = CURRENT_SCOPE
        .scope(js, async {
            thunk.call(&[], &mut ContBarrier::new()).await
        })
        .await?;

    loop {
        let mut js = std::mem::take(&mut *join_set.lock().unwrap());
        if js.is_empty() {
            break;
        }
        while let Some(res) = js.join_next().await {
            if let Err(e) = res {
                if e.is_panic() {
                    return Err(Exception::error("spawned task panicked"));
                }
            }
        }
    }

    Ok(result)
}

#[bridge(name = "%run-tasks", lib = "(cml tasks bridge)")]
pub async fn run_tasks(thunk: Procedure) -> Result<Vec<Value>, Exception> {
    run_tasks_impl(thunk).await
}

#[cps_bridge(def = "%spawn task", lib = "(cml tasks bridge)")]
pub fn spawn_task(
    _runtime: &Runtime,
    _env: &[Value],
    args: &[Value],
    _rest_args: &[Value],
    barrier: &mut ContBarrier,
    k: Value,
) -> Result<Application, Exception> {
    let task: Procedure = args[0].clone().try_into()?;
    let saved = barrier.save();

    CURRENT_SCOPE
        .try_with(|scope| {
            let scope = Arc::clone(scope);
            scope.lock().unwrap().spawn(async move {
                let mut barrier = ContBarrier::from(saved);
                let _ = task.call(&[], &mut barrier).await;
            });
        })
        .map_err(|_| Exception::error("spawn: not inside run-tasks"))?;

    Ok(Application::new(k.try_into().unwrap(), vec![]))
}

#[bridge(name = "%yield", lib = "(cml tasks bridge)")]
pub async fn yield_now() -> Result<Vec<Value>, Exception> {
    tokio::task::yield_now().await;
    Ok(vec![])
}
