use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
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
) -> Result<Json<VoteResponse<ExampleNodeId>>, StatusCode> {
    if target_uuid.0 == app.id {
        let res = app.raft.vote(vote).await.unwrap();
        Ok(Json(res))
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

pub async fn append(
    State(app): State<Arc<ExampleApp>>,
    TypedHeader(target_uuid): TypedHeader<XRaftTargetUuid>,
    Json(log): Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> Result<Json<AppendEntriesResponse<ExampleNodeId>>, StatusCode> {
    if target_uuid.0 == app.id {
        let res = app.raft.append_entries(log).await.unwrap();
        Ok(Json(res))
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

pub async fn snapshot(
    State(app): State<Arc<ExampleApp>>,
    TypedHeader(target_uuid): TypedHeader<XRaftTargetUuid>,
    Json(snapshot_request): Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> Result<Json<InstallSnapshotResponse<ExampleNodeId>>, StatusCode> {
    if target_uuid.0 == app.id {
        let res = app.raft.install_snapshot(snapshot_request).await.unwrap();
        Ok(Json(res))
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}
