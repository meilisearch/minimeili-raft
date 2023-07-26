use openraft::{BasicNode, SnapshotMeta};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use super::ExampleNodeId;

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For example the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExampleRequest {
    ProcessThat { task_ids: RoaringBitmap },
    LongTask { duration_sec: u64 },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this example it will return a optional value from a given key in
 * the `ExampleRequest.Set`.
 *
 * TODO: Should we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExampleResponse {
    pub new_task_id: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExampleSnapshot {
    pub meta: SnapshotMeta<ExampleNodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}
