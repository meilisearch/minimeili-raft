use std::ops::RangeBounds;
use std::path::Path;

use heed::byteorder::BE;
use heed::types::{DecodeIgnore, SerdeJson, Str, U64};
use heed::{Database, Env, EnvOpenOptions, PolyDatabase, RoTxn, RwTxn};
use openraft::{BasicNode, Entry, LogId, StoredMembership, Vote};
use uuid::Uuid;

use crate::raft::store::ExampleSnapshot;
use crate::raft::{ExampleNodeId, ExampleTypeConfig};

#[derive(Debug, Clone)]
pub struct RaftDatabase {
    env: Env,
    main: PolyDatabase,
    logs: Database<U64<BE>, SerdeJson<Entry<ExampleTypeConfig>>>,
}

impl RaftDatabase {
    pub fn open_or_create(path: impl AsRef<Path>) -> heed::Result<RaftDatabase> {
        let env = EnvOpenOptions::new()
            .map_size(2 * 1024 * 1024 * 1024) // 2GiB
            .max_dbs(3)
            .open(path)?;

        let mut wtxn = env.write_txn()?;
        let main = env.create_poly_database(&mut wtxn, Some("main"))?;
        let logs = env.create_database(&mut wtxn, Some("logs"))?;

        // We check if there is an UUID in the database
        if main.get::<Str, DecodeIgnore>(&wtxn, "uuid")?.is_none() {
            main.put::<Str, SerdeJson<Uuid>>(&mut wtxn, "uuid", &Uuid::new_v4())?;
        }

        wtxn.commit()?;

        Ok(RaftDatabase { env, main, logs })
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    pub fn uuid(&self, rtxn: &RoTxn) -> heed::Result<Uuid> {
        Ok(self.main.get::<Str, SerdeJson<Uuid>>(rtxn, "uuid")?.unwrap())
    }

    pub fn last_membership(
        &self,
        rtxn: &RoTxn,
    ) -> heed::Result<Option<StoredMembership<ExampleNodeId, BasicNode>>> {
        self.main.get::<Str, SerdeJson<_>>(rtxn, "last-membership")
    }

    pub fn put_last_membership(
        &self,
        wtxn: &mut RwTxn,
        membership: &StoredMembership<ExampleNodeId, BasicNode>,
    ) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<_>>(wtxn, "last-membership", membership)
    }

    pub fn last_applied_log(&self, rtxn: &RoTxn) -> heed::Result<Option<LogId<ExampleNodeId>>> {
        self.main.get::<Str, SerdeJson<_>>(rtxn, "last-applied-log")
    }

    pub fn put_last_applied_log(
        &self,
        wtxn: &mut RwTxn,
        log: &LogId<ExampleNodeId>,
    ) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<_>>(wtxn, "last-applied-log", log)
    }

    pub fn vote(&self, rtxn: &RoTxn) -> heed::Result<Option<Vote<ExampleNodeId>>> {
        self.main.get::<Str, SerdeJson<_>>(rtxn, "vote")
    }

    pub fn put_vote(&self, wtxn: &mut RwTxn, log: &Vote<ExampleNodeId>) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<_>>(wtxn, "vote", log)
    }

    pub fn append_to_logs(
        &self,
        wtxn: &mut RwTxn,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> heed::Result<()> {
        for entry in entries {
            self.logs.put(wtxn, &entry.log_id.index, entry)?
        }
        Ok(())
    }

    pub fn delete_conflict_logs_since(
        &self,
        wtxn: &mut RwTxn,
        log_id: &LogId<ExampleNodeId>,
    ) -> heed::Result<()> {
        let indexes_to_delete: heed::Result<Vec<_>> = self
            .logs
            .range(wtxn, &(log_id.index..))?
            .remap_data_type::<DecodeIgnore>()
            .map(|r| r.map(|(index, ..)| index))
            .collect();

        for index in indexes_to_delete? {
            self.logs.delete(wtxn, &index)?;
        }

        Ok(())
    }

    pub fn purge_logs_upto(
        &self,
        wtxn: &mut RwTxn,
        log_id: &LogId<ExampleNodeId>,
    ) -> heed::Result<()> {
        let ld = self.last_purged_log_id(wtxn)?;
        assert!(ld <= Some(*log_id));
        self.put_last_purged_log_id(wtxn, log_id)?;

        let indexes_to_delete: heed::Result<Vec<_>> = self
            .logs
            .range(wtxn, &(..=log_id.index))?
            .remap_data_type::<DecodeIgnore>()
            .map(|r| r.map(|(index, ..)| index))
            .collect();

        for index in indexes_to_delete? {
            self.logs.delete(wtxn, &index)?;
        }

        Ok(())
    }

    pub fn last_purged_log_id(&self, rtxn: &RoTxn) -> heed::Result<Option<LogId<Uuid>>> {
        self.main.get::<Str, SerdeJson<_>>(rtxn, "last-purged-log-id")
    }

    pub fn put_last_purged_log_id(
        &self,
        wtxn: &mut RwTxn,
        log_id: &LogId<Uuid>,
    ) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<_>>(wtxn, "last-purged-log-id", log_id)
    }

    pub fn log_entries(
        &self,
        rtxn: &RoTxn,
        range: impl RangeBounds<u64>,
    ) -> heed::Result<Vec<Entry<ExampleTypeConfig>>> {
        self.logs.range(rtxn, &range)?.map(|r| r.map(|(_, entry)| entry)).collect()
    }

    pub fn last_log_id(&self, rtxn: &RoTxn) -> heed::Result<Option<LogId<Uuid>>> {
        self.logs.last(rtxn).map(|r| r.map(|(_, entry)| entry.log_id))
    }

    pub fn put_current_snapshot(
        &self,
        wtxn: &mut RwTxn,
        snapshot: &ExampleSnapshot,
    ) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<_>>(wtxn, "snapshot", snapshot)
    }

    pub fn current_snapshot(&self, rtxn: &RoTxn) -> heed::Result<Option<ExampleSnapshot>> {
        self.main.get::<Str, SerdeJson<_>>(rtxn, "snapshot")
    }

    pub fn snapshot_index(&self, rtxn: &RoTxn) -> heed::Result<u64> {
        self.main.get::<Str, U64<BE>>(rtxn, "snapshot-index").map(Option::unwrap_or_default)
    }

    pub fn put_snapshot_index(&self, wtxn: &mut RwTxn, index: u64) -> heed::Result<()> {
        self.main.put::<Str, U64<BE>>(wtxn, "snapshot-index", &index)
    }
}
