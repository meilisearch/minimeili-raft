mod index;
mod raft;

use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::{fmt, fs};

use async_trait::async_trait;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};

pub use self::index::IndexDatabase;
pub use self::raft::RaftDatabase;
use crate::database::index::SleepOperation;
use crate::raft::store::{ExampleRequest, ExampleResponse, ExampleSnapshot};
use crate::raft::{ExampleNodeId, ExampleRaft, ExampleTypeConfig};

#[derive(Clone)]
pub struct Database {
    pub raft: RaftDatabase,
    pub index: IndexDatabase,
}

impl Database {
    pub fn open_or_create(path: impl AsRef<Path>, raft: ExampleRaft) -> anyhow::Result<Database> {
        let raft_path = path.as_ref().join("raft");
        let index_path = path.as_ref().join("index");

        fs::create_dir_all(&raft_path)?;
        fs::create_dir_all(&index_path)?;

        Ok(Database {
            raft: RaftDatabase::open_or_create(raft_path)?,
            index: IndexDatabase::open_or_create(index_path, raft)?,
        })
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
        let mut wtxn = self.raft.write_txn().unwrap();
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
        self.clone()
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
        let last_membership = self.raft.last_membership(&rtxn).unwrap().unwrap_or_default();
        Ok((last_applied_log, last_membership))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<ExampleResponse>, StorageError<ExampleNodeId>> {
        let mut raft_wtxn = self.raft.write_txn().unwrap();
        let mut index_wtxn = self.index.write_txn().unwrap();
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to state machine");

            self.raft.put_last_applied_log(&mut raft_wtxn, &entry.log_id).unwrap();

            match entry.payload {
                EntryPayload::Blank => responses.push(ExampleResponse { new_task_id: None }),
                EntryPayload::Normal(ref req) => match req {
                    ExampleRequest::ProcessThat { task_ids } => {
                        tracing::debug!(%entry.log_id, "computing multiple tasks {:?}", task_ids);
                        self.index.process_that(&mut index_wtxn, task_ids).unwrap();
                        responses.push(ExampleResponse { new_task_id: None }); // TODO make it more clear
                    }
                    ExampleRequest::LongTask { duration_sec } => {
                        let operation = SleepOperation { time_in_seconds: *duration_sec };
                        tracing::debug!(%entry.log_id, "insert new operation {:?}", operation);
                        let task =
                            self.index.insert_new_operation(&mut index_wtxn, &operation).unwrap();
                        responses.push(ExampleResponse { new_task_id: Some(task) });
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    let membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    self.raft.put_last_membership(&mut raft_wtxn, &membership).unwrap();
                    responses.push(ExampleResponse { new_task_id: None })
                }
            };
        }

        // TODO what happens if the first txn succeed but not the second?
        // TODO map the error with `StorageIOError`
        index_wtxn.commit().unwrap();
        raft_wtxn.commit().unwrap();

        Ok(responses)
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
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = ExampleSnapshot { meta: meta.clone(), data: snapshot.into_inner() };

        // Update the state machine.
        let mut index_wtxn = self.index.write_txn().unwrap();
        // TODO map the error into a `StorageIOError`
        // TODO prefer directly using the raw bytes
        self.index
            .import_dump_from_reader(&mut index_wtxn, Cursor::new(&new_snapshot.data[..]))
            .unwrap();

        // Update current snapshot in the Raft store.
        let mut raft_wtxn = self.raft.write_txn().unwrap();
        // TODO it's too big to store in LMDB
        self.raft.put_current_snapshot(&mut raft_wtxn, &new_snapshot).unwrap();

        // TODO what should we do if the first txn commits but not the second one?
        index_wtxn.commit().unwrap();
        raft_wtxn.commit().unwrap();

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<ExampleNodeId, BasicNode, Self::SnapshotData>>,
        StorageError<ExampleNodeId>,
    > {
        let rtxn = self.raft.read_txn().unwrap();
        match self.raft.current_snapshot(&rtxn).unwrap() {
            Some(ExampleSnapshot { meta, data }) => {
                Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) }))
            }
            None => Ok(None),
        }
    }
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
impl RaftSnapshotBuilder<ExampleTypeConfig, Cursor<Vec<u8>>> for Database {
    /// Build snapshot
    ///
    /// A snapshot has to contain information about exactly all logs up to the last applied.
    ///
    /// Building snapshot can be done by:
    /// - Performing log compaction, e.g. merge log entries that operates on the same key, like a
    ///   LSM-tree does,
    /// - or by fetching a snapshot from the state machine.
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ExampleNodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<ExampleNodeId>>
    {
        let index_rtxn = self.index.read_txn().unwrap();
        let mut raft_wtxn = self.raft.write_txn().unwrap();

        // Serialize the data of the state machine.
        let mut data = Vec::new();
        // TODO map the errors in a `StorageIOError`
        self.index.extract_dump_to_writer(&index_rtxn, &mut data).unwrap();

        let last_applied_log = self.raft.last_applied_log(&raft_wtxn).unwrap();
        let last_membership = self.raft.last_membership(&raft_wtxn).unwrap().unwrap();

        let snapshot_index = self.raft.snapshot_index(&raft_wtxn).unwrap() + 1;
        self.raft.put_snapshot_index(&mut raft_wtxn, snapshot_index).unwrap();

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_index)
        } else {
            format!("--{}", snapshot_index)
        };

        let meta = SnapshotMeta { last_log_id: last_applied_log, last_membership, snapshot_id };
        let snapshot = ExampleSnapshot { meta: meta.clone(), data: data.clone() };
        self.raft.put_current_snapshot(&mut raft_wtxn, &snapshot).unwrap();

        raft_wtxn.commit().unwrap();

        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }

    // NOTES:
    // This interface is geared toward small file-based snapshots. However, not all snapshots can
    // be easily represented as a file. Probably a more generic interface will be needed to address
    // also other needs.
}
