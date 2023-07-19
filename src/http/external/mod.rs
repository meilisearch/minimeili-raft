use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{debug_handler, Json, Router};
use serde::Serialize;

use crate::raft::app::ExampleApp;

/// The external app router.
pub fn app(state: Arc<ExampleApp>) -> Router<(), Body> {
    Router::new().route("/show/:duration", get(show)).with_state(state)
}

#[debug_handler]
async fn show(State(app): State<Arc<ExampleApp>>, Path(duration_sec): Path<u64>) -> Json<Answer> {
    let rtxn = app.database.index.read_txn().unwrap();
    let tasks = app.database.index.show_tasks_of_duration(&rtxn, duration_sec).unwrap();
    Json(Answer { duration_sec, tasks: tasks.into_iter().collect() })
}

#[derive(Debug, Clone, Serialize)]
pub struct Answer {
    pub duration_sec: u64,
    pub tasks: Vec<u32>,
}
