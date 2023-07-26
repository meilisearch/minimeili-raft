use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{ExampleNodeId, ExampleTypeConfig};
use crate::http::internal::X_RAFT_TARGET_UUID;

pub struct ExampleNetwork {}

impl ExampleNetwork {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: &ExampleNodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<ExampleNodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned + std::fmt::Debug,
    {
        let addr = &target_node.addr;

        let url = format!("http://{}/{}", addr, uri);
        tracing::debug!("send_rpc to url: {}", url);
        let client = reqwest::Client::new();
        tracing::debug!("client is created for: {}", url);

        let resp = client
            .post(url)
            .header(&X_RAFT_TARGET_UUID, target.to_string())
            .json(&req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // TODO react to a bad request and remove node from the cluster if too many/first of them.

        tracing::debug!("client.post() is sent");

        resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
#[async_trait]
impl RaftNetworkFactory<ExampleTypeConfig> for ExampleNetwork {
    type Network = ExampleNetworkConnection;

    async fn new_client(&mut self, target: ExampleNodeId, node: &BasicNode) -> Self::Network {
        ExampleNetworkConnection { owner: ExampleNetwork {}, target, target_node: node.clone() }
    }
}

pub struct ExampleNetworkConnection {
    owner: ExampleNetwork,
    target: ExampleNodeId,
    target_node: BasicNode,
}

#[async_trait]
impl RaftNetwork<ExampleTypeConfig> for ExampleNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<ExampleTypeConfig>,
    ) -> Result<
        AppendEntriesResponse<ExampleNodeId>,
        RPCError<ExampleNodeId, BasicNode, RaftError<ExampleNodeId>>,
    > {
        self.owner.send_rpc(&self.target, &self.target_node, "raft/append", req).await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<ExampleTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<ExampleNodeId>,
        RPCError<ExampleNodeId, BasicNode, RaftError<ExampleNodeId, InstallSnapshotError>>,
    > {
        self.owner.send_rpc(&self.target, &self.target_node, "raft/snapshot", req).await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<ExampleNodeId>,
    ) -> Result<
        VoteResponse<ExampleNodeId>,
        RPCError<ExampleNodeId, BasicNode, RaftError<ExampleNodeId>>,
    > {
        self.owner.send_rpc(&self.target, &self.target_node, "raft/vote", req).await
    }
}
