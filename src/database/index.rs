use std::io;
use std::path::Path;
use std::time::Duration;

use heed::byteorder::BE;
use heed::types::{ByteSlice, SerdeJson, Str, U32, U64};
use heed::{Env, EnvOpenOptions, PolyDatabase, RoTxn, RwTxn};
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use synchronoise::SignalEvent;

static PRODUCER_SIGNAL_EVENT: OnceCell<SignalEvent> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct IndexDatabase {
    env: Env,
    main: PolyDatabase,
    tasks: heed::Database<U32<BE>, SerdeJson<SleepOperation>>,
    content: heed::Database<U64<BE>, SerdeJson<RoaringBitmap>>,
}

impl IndexDatabase {
    pub fn open_or_create(path: impl AsRef<Path>) -> heed::Result<IndexDatabase> {
        let env = EnvOpenOptions::new()
            .map_size(20 * 1024 * 1024 * 1024) // 20GiB
            .max_dbs(3)
            .open(path)?;

        let mut wtxn = env.write_txn()?;
        let main = env.create_poly_database(&mut wtxn, Some("main"))?;
        let tasks = env.create_database(&mut wtxn, Some("tasks"))?;
        let content = env.create_database(&mut wtxn, Some("content"))?;
        wtxn.commit()?;

        let database = IndexDatabase { env, main, tasks, content };
        let _signal = PRODUCER_SIGNAL_EVENT.get_or_init(|| {
            let database = database.clone();
            std::thread::spawn(|| await_and_process_new_operations(database).unwrap());
            SignalEvent::auto(true)
        });

        Ok(database)
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    /// That is the simulation of a data to insert.
    pub fn show_tasks_of_duration(
        &self,
        rtxn: &RoTxn,
        duration_sec: u64,
    ) -> heed::Result<RoaringBitmap> {
        Ok(self.content.get(rtxn, &duration_sec)?.unwrap_or_default())
    }

    fn next_task_id(&self, rtxn: &RoTxn) -> heed::Result<Option<u32>> {
        Ok(self.enqueued_tasks(rtxn)?.max().and_then(|x| x.checked_add(1)))
    }

    pub fn insert_new_operation(
        &self,
        wtxn: &mut RwTxn,
        operation: &SleepOperation,
    ) -> heed::Result<u32> {
        let new_task_id = self.next_task_id(wtxn)?.expect("no more task id available");
        self.tasks.put(wtxn, &new_task_id, operation)?;

        // We insert the new task id in the list of all tasks
        let mut bitmap = self.all_tasks(wtxn)?;
        bitmap.insert(new_task_id);
        self.put_all_tasks(wtxn, &bitmap)?;

        // We signal that there is somethign new to take care about.
        // We send the signal now even, so that the thread is unblocked
        // and will block on the write_txn method.
        PRODUCER_SIGNAL_EVENT.wait().signal();

        Ok(new_task_id)
    }

    /// Export the content of the database into a `io::Write`.
    pub fn extract_dump_to_writer<W: io::Write>(
        &self,
        rtxn: &RoTxn,
        mut writer: W,
    ) -> anyhow::Result<W> {
        let IndexDatabase { env: _, main, tasks, content } = self;

        let main: heed::Result<Vec<_>> = main
            .iter::<ByteSlice, ByteSlice>(rtxn)?
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect();
        let tasks: heed::Result<Vec<_>> = tasks.iter(rtxn)?.collect();
        let content: heed::Result<Vec<_>> = content.iter(rtxn)?.collect();
        let dump = Dump { main: main?, tasks: tasks?, content: content? };

        serde_json::to_writer(&mut writer, &dump)?;

        Ok(writer)
    }

    /// Erase the database and load the dumps
    pub fn import_dump_from_reader<R: io::Read>(
        &self,
        wtxn: &mut RwTxn,
        reader: R,
    ) -> anyhow::Result<()> {
        let IndexDatabase { env: _, main, tasks, content } = self;
        let Dump { main: dump_main, tasks: dump_tasks, content: dump_content } =
            serde_json::from_reader(reader)?;

        // Clean the database
        main.clear(wtxn)?;
        tasks.clear(wtxn)?;
        content.clear(wtxn)?;

        // Fill the database
        for (key, value) in dump_main {
            main.put::<ByteSlice, ByteSlice>(wtxn, &key, &value)?;
        }

        // Fill the database
        for (task_id, operation) in dump_tasks {
            tasks.put(wtxn, &task_id, &operation)?;
        }

        for (word, bitmap) in dump_content {
            content.put(wtxn, &word, &bitmap)?;
        }

        Ok(())
    }

    fn enqueued_tasks(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self.all_tasks(rtxn)? - self.processed_tasks(rtxn)?)
    }

    fn all_tasks(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self.main.get::<Str, SerdeJson<RoaringBitmap>>(rtxn, "all-tasks")?.unwrap_or_default())
    }

    fn put_all_tasks(&self, wtxn: &mut RwTxn, bitmap: &RoaringBitmap) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<RoaringBitmap>>(wtxn, "all-tasks", bitmap)
    }

    fn processed_tasks(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .get::<Str, SerdeJson<RoaringBitmap>>(rtxn, "processed-tasks")?
            .unwrap_or_default())
    }

    fn put_processed_tasks(&self, wtxn: &mut RwTxn, bitmap: &RoaringBitmap) -> heed::Result<()> {
        self.main.put::<Str, SerdeJson<RoaringBitmap>>(wtxn, "processed-tasks", bitmap)
    }
}

fn await_and_process_new_operations(database: IndexDatabase) -> heed::Result<()> {
    loop {
        // We wait for the OnceCell to init and wait for the signal to be true
        PRODUCER_SIGNAL_EVENT.wait().wait();

        // We process ALL the new enqueued tasks and break when nothing new is found
        loop {
            // Once we found that there is a new task to process we find the new ones
            // to process and pick the smallest one.
            let mut wtxn = database.write_txn()?;
            let to_process = match database.enqueued_tasks(&wtxn)?.min() {
                Some(task_id) => task_id,
                None => break,
            };

            let SleepOperation { time_in_seconds } =
                database.tasks.get(&wtxn, &to_process)?.expect("an operation must always exists");
            let duration = Duration::from_secs(time_in_seconds);
            tracing::info!("Processing {} for {:.02?}...", to_process, duration);
            std::thread::sleep(duration);

            // We store the id of the task that took this amount of time.
            let duration_sec = duration.as_secs();
            let mut bitmap = database.content.get(&wtxn, &duration_sec)?.unwrap_or_default();
            bitmap.insert(to_process);
            database.content.put(&mut wtxn, &duration_sec, &bitmap)?;

            // Once we finish processing the task we write it in the tasks list.
            let mut bitmap = database.processed_tasks(&wtxn)?;
            bitmap.insert(to_process);
            database.put_processed_tasks(&mut wtxn, &bitmap)?;
            tracing::info!("Processed {} which took {:.02?}!", to_process, duration);

            // TODO talk about this with Louis
            wtxn.commit()?;
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dump {
    pub main: Vec<(Vec<u8>, Vec<u8>)>,
    pub tasks: Vec<(u32, SleepOperation)>,
    pub content: Vec<(u64, RoaringBitmap)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SleepOperation {
    pub time_in_seconds: u64,
}
