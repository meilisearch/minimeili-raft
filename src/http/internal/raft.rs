use std::sync::Arc;

use axum::extract::State;
use axum::{Json, TypedHeader};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::raft_target_uuid::XRaftTargetUuid;
use crate::raft::app::ExampleApp;
use crate::raft::{ExampleNodeId, ExampleTypeConfig};

// --- Raft communication

pub async fn vote(
    State(app): State<Arc<ExampleApp>>,
    TypedHeader(target_uuid): TypedHeader<XRaftTargetUuid>,
    Json(vote): Json<VoteRequest<ExampleNodeId>>,
) -> Json<VoteResponse<ExampleNodeId>> {
    assert_eq!(target_uuid.0, app.id);
    let res = app.raft.vote(vote).await.unwrap();
    Json(res)
}

pub async fn append(
    State(app): State<Arc<ExampleApp>>,
    TypedHeader(target_uuid): TypedHeader<XRaftTargetUuid>,
    Json(log): Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> Json<AppendEntriesResponse<ExampleNodeId>> {
    assert_eq!(target_uuid.0, app.id);
    let res = app.raft.append_entries(log).await.unwrap();
    Json(res)
}

pub async fn snapshot(
    State(app): State<Arc<ExampleApp>>,
    TypedHeader(target_uuid): TypedHeader<XRaftTargetUuid>,
    Json(snapshot_request): Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> Json<InstallSnapshotResponse<ExampleNodeId>> {
    assert_eq!(target_uuid.0, app.id);
    let res = app.raft.install_snapshot(snapshot_request).await.unwrap();
    Json(res)
}
