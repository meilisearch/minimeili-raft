use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use openraft::raft::ClientWriteResponse;
use openraft::{BasicNode, RaftMetrics};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::raft::app::ExampleApp;
use crate::raft::{ExampleNodeId, ExampleTypeConfig};

// --- Cluster management

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
pub async fn add_learner(
    State(app): State<Arc<ExampleApp>>,
    Json(node_addr): Json<String>,
) -> Json<ClientWriteResponse<ExampleTypeConfig>> {
    let uuid: Uuid = reqwest::get(format!("http://{node_addr}/cluster/uuid"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let node = BasicNode { addr: node_addr };
    let res = app.raft.add_learner(uuid, node, false).await.unwrap();
    Json(res)
}

pub async fn uuid(State(app): State<Arc<ExampleApp>>) -> Json<Uuid> {
    Json(app.id)
}

/// Changes specified learners to members, or remove members.
pub async fn change_membership(
    State(app): State<Arc<ExampleApp>>,
    Json(node_uuid): Json<Uuid>,
) -> Json<ClientWriteResponse<ExampleTypeConfig>> {
    let last_membership = {
        let rtxn = app.database.raft.read_txn().unwrap();
        app.database.raft.last_membership(&rtxn).unwrap()
    };
    let res = app
        .raft
        .change_membership(
            last_membership
                .unwrap()
                .nodes()
                .map(|(node_uuid, _node)| *node_uuid)
                .chain(std::iter::once(node_uuid)),
            true,
        )
        .await
        .unwrap();
    Json(res)
}

pub async fn get_membership(
    State(app): State<Arc<ExampleApp>>,
) -> Json<Vec<(ExampleNodeId, String, NodeRole)>> {
    let last_membership = {
        let rtxn = app.database.raft.read_txn().unwrap();
        app.database.raft.last_membership(&rtxn).unwrap()
    };

    let last_membership = last_membership.unwrap();
    let membership = last_membership.membership();
    // get_node.unwrap cannot fail, is in the list of voters
    let mut nodes: Vec<_> = membership
        .voter_ids()
        .map(|voter_uuid| {
            (voter_uuid, membership.get_node(&voter_uuid).unwrap().addr.clone(), NodeRole::Voter)
        })
        .collect();
    nodes.extend(membership.learner_ids().map(|learner_uuid| {
        (learner_uuid, membership.get_node(&learner_uuid).unwrap().addr.clone(), NodeRole::Learner)
    }));
    Json(nodes)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NodeRole {
    Voter,
    Learner,
}

/// Get the latest metrics of the cluster
pub async fn metrics(
    State(app): State<Arc<ExampleApp>>,
) -> Json<RaftMetrics<ExampleNodeId, BasicNode>> {
    let metrics = app.raft.metrics().borrow().clone();
    Json(metrics)
}
