use std::sync::Arc;

use axum::body::Body;
use axum::routing::{get, post, put};
use axum::Router;
pub use raft_target_uuid::X_RAFT_TARGET_UUID;

use crate::raft::app::ExampleApp;

mod cluster;
mod raft;
mod raft_target_uuid;

/// Our internal app router.
pub fn app(state: Arc<ExampleApp>) -> Router<(), Body> {
    Router::new()
        .route("/raft/vote", post(raft::vote))
        .route("/raft/append", post(raft::append))
        .route("/raft/snapshot", post(raft::snapshot))
        .route("/cluster/learner", put(cluster::add_learner))
        .route("/cluster/membership", get(cluster::get_membership))
        .route("/cluster/membership", post(cluster::change_membership))
        .route("/cluster/uuid", get(cluster::uuid))
        .route("/cluster/metrics", get(cluster::metrics))
        .with_state(state)
}
