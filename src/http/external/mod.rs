use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use roaring::RoaringBitmap;
use serde::Serialize;

use crate::raft::app::ExampleApp;

/// The external app router.
pub fn app(state: Arc<ExampleApp>) -> Router<Arc<ExampleApp>, Body> {
    Router::new().route("/show/:duration", get(show)).with_state(state)
}

async fn show(State(app): State<Arc<ExampleApp>>, Path(duration_sec): Path<u64>) -> Json<Answer> {
    let rtxn = app.database.read_txn().unwrap();
    let tasks = app.database.show_tasks_of_duration(&rtxn, duration_sec).unwrap();
    Json(Answer { duration_sec, tasks })
}

#[derive(Debug, Clone, Serialize)]
pub struct Answer {
    pub duration_sec: u64,
    pub tasks: RoaringBitmap,
}
