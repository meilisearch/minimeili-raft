use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft::app::ExampleApp;
use crate::raft::{ExampleNodeId, ExampleTypeConfig};

// --- Raft communication

pub async fn vote(
    State(app): State<Arc<ExampleApp>>,
    Json(vote): Json<VoteRequest<ExampleNodeId>>,
) -> Json<VoteResponse<ExampleNodeId>> {
    let res = app.raft.vote(vote).await.unwrap();
    Json(res)
}

pub async fn append(
    State(app): State<Arc<ExampleApp>>,
    Json(log): Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> Json<AppendEntriesResponse<ExampleNodeId>> {
    let res = app.raft.append_entries(log).await.unwrap();
    Json(res)
}

pub async fn snapshot(
    State(app): State<Arc<ExampleApp>>,
    Json(snapshot_request): Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> Json<InstallSnapshotResponse<ExampleNodeId>> {
    let res = app.raft.install_snapshot(snapshot_request).await.unwrap();
    Json(res)
}
