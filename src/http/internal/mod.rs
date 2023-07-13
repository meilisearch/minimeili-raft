use std::sync::Arc;

use axum::body::Body;
use axum::routing::post;
use axum::Router;

use crate::raft::app::ExampleApp;

mod cluster;
mod raft;

/// Our internal app router.
pub fn app(state: Arc<ExampleApp>) -> Router<(), Body> {
    Router::new()
        .route("/raft/vote", post(raft::vote))
        .route("/raft/append", post(raft::append))
        .route("/raft/snapshot", post(raft::snapshot))
        .with_state(state)
}
