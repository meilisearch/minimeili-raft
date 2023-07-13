use std::sync::Arc;

use openraft::Config;

use super::{ExampleNodeId, ExampleRaft, ExampleStore};
use crate::database::Database;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct ExampleApp {
    pub id: ExampleNodeId,
    pub database: Database,
    pub addr: String,
    pub raft: ExampleRaft,
    pub store: Arc<ExampleStore>,
    pub config: Arc<Config>,
}
