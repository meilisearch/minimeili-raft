use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use crate::raft::app::ExampleApp;
use crate::raft::store::ExampleRequest;

/// The external app router.
pub fn app(state: Arc<ExampleApp>) -> Router<(), Body> {
    Router::new()
        .route("/show/:duration", get(show))
        .route("/enqueue/:duration", post(enqueue))
        .with_state(state)
}

async fn show(State(app): State<Arc<ExampleApp>>, Path(duration_sec): Path<u64>) -> Json<Answer> {
    let rtxn = app.database.index.read_txn().unwrap();
    let tasks = app.database.index.show_tasks_of_duration(&rtxn, duration_sec).unwrap();
    Json(Answer { duration_sec, tasks: tasks.into_iter().collect() })
}

/// Enqueues a task in the task queue for it to be executed by the whole cluster later.
async fn enqueue(State(app): State<Arc<ExampleApp>>, Path(duration_sec): Path<u64>) {
    let response = app.raft.client_write(ExampleRequest::LongTask { duration_sec }).await.unwrap();
    dbg!(response);
}

#[derive(Debug, Clone, Serialize)]
pub struct Answer {
    pub duration_sec: u64,
    pub tasks: Vec<u32>,
}
