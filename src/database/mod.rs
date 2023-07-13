mod index;
mod raft;

use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;

use async_trait::async_trait;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftStorage, Snapshot,
    StorageError, StoredMembership, Vote,
};

pub use self::index::IndexDatabase;
pub use self::raft::RaftDatabase;
use crate::raft::store::{ExampleRequest, ExampleResponse};
use crate::raft::{ExampleNodeId, ExampleTypeConfig};

#[derive(Clone)]
pub struct Database {
    pub raft: RaftDatabase,
    pub index: IndexDatabase,
}

#[async_trait]
impl RaftLogReader<ExampleTypeConfig> for Database {
    /// Returns the last deleted log id and the last log id.
    ///
    /// The impl should not consider the applied log id in state machine.
    /// The returned `last_log_id` could be the log id of the last present log entry, or the
    /// `last_purged_log_id` if there is no entry at all.
    // NOTE: This can be made into sync, provided all state machines will use atomic read or the
    // like.
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<ExampleTypeConfig>, StorageError<ExampleNodeId>> {
        let rtxn = self.raft.read_txn().unwrap();
        let last_log_id = self.raft.last_log_id(&rtxn).unwrap();
        let last_purged_log_id = self.raft.last_purged_log_id(&rtxn).unwrap();
        let last_log_id = last_log_id.or(last_purged_log_id);
        Ok(LogState { last_purged_log_id, last_log_id })
    }

    /// Get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start,
    /// stop)`.
    ///
    /// Entry that is not found is allowed.
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ExampleTypeConfig>>, StorageError<ExampleNodeId>> {
        let rtxn = self.raft.read_txn().unwrap();
        let entries = self.raft.log_entries(&rtxn, range).unwrap();
        Ok(entries)
    }
}

#[async_trait]
impl RaftStorage<ExampleTypeConfig> for Database {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        let wtxn = self.raft.write_txn().unwrap();
        self.raft.put_vote(&mut wtxn, vote).unwrap();
        wtxn.commit().unwrap();
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ExampleNodeId>>, StorageError<ExampleNodeId>> {
        let rtxn = self.raft.read_txn().unwrap();
        Ok(self.raft.vote(&rtxn).unwrap())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<(), StorageError<ExampleNodeId>> {
        let mut wtxn = self.raft.write_txn().unwrap();
        self.raft.append_to_logs(&mut wtxn, entries).unwrap();
        wtxn.commit().unwrap();

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let mut wtxn = self.raft.write_txn().unwrap();
        self.raft.delete_conflict_logs_since(&mut wtxn, &log_id).unwrap();
        wtxn.commit().unwrap();

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<ExampleNodeId>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let mut wtxn = self.raft.write_txn().unwrap();
        self.raft.purge_logs_upto(&mut wtxn, &log_id).unwrap();
        wtxn.commit().unwrap();

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<ExampleNodeId>>, StoredMembership<ExampleNodeId, BasicNode>),
        StorageError<ExampleNodeId>,
    > {
        let rtxn = self.raft.read_txn().unwrap();
        let last_applied_log = self.raft.last_applied_log(&rtxn).unwrap();
        let last_membership = self.raft.last_membership(&rtxn).unwrap();
        Ok((last_applied_log, last_membership))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<ExampleResponse>, StorageError<ExampleNodeId>> {
        todo!();
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(ExampleResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    ExampleRequest::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(ExampleResponse { value: Some(value.clone()) })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(ExampleResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<ExampleNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ExampleNodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<ExampleNodeId>> {
        todo!();
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = ExampleSnapshot { meta: meta.clone(), data: snapshot.into_inner() };

        // Update the state machine.
        {
            let updated_state_machine: ExampleStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<ExampleNodeId, BasicNode, Self::SnapshotData>>,
        StorageError<ExampleNodeId>,
    > {
        todo!();
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
