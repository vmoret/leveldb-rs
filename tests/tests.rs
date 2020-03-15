use tempdir::TempDir;

use leveldb::*;

#[test]
fn test_options() {
    Options::default();
}

#[test]
fn test_db_open() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("create_if_missing").unwrap();
    let res: Result<DB, Error> = DB::open(tmpdir.path(), Some(options));
    assert!(res.is_ok());
}

#[test]
fn test_db_open_non_existant_without_create() {
    let mut options = Options::default();
    options.create_if_missing = false;
    let tmpdir = TempDir::new("missing").unwrap();
    let res: Result<DB, Error> = DB::open(tmpdir.path(), Some(options));
    assert!(res.is_err());
}

#[test]
fn test_db_open_with_cache() {
    let mut options = Options::default();
    options.create_if_missing = true;
    options.block_cache = Some(Cache::new(20));
    let tmpdir = TempDir::new("open_with_cache").unwrap();
    let res: Result<DB, Error> = DB::open(tmpdir.path(), Some(options));
    assert!(res.is_ok());
}

#[test]
fn test_comparator() {
    let mut options = Options::default();
    options.create_if_missing = true;
    options.comparator = Some(Box::new(ReverseComparator));
    let tmpdir = TempDir::new("reversed_comparator").unwrap();
    let db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let options = WriteOptions::default();
    db.put(&options, &[1u8], &[1u8]).unwrap();
    db.put(&options, &[2u8], &[2u8]).unwrap();

    let options = ReadOptions::default();
    let mut iter = db.iter(&options);

    assert_eq!((vec![2], vec![2]), iter.next().unwrap());
    assert_eq!((vec![1], vec![1]), iter.next().unwrap());
}

#[test]
fn test_db_compact() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("db_compact").unwrap();
    let mut db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let options = WriteOptions::default();
    db.put(&options, &[1u8], &[1u8]).unwrap();
    db.put(&options, &[2u8], &[2u8]).unwrap();
    db.put(&options, &[3u8], &[3u8]).unwrap();
    db.put(&options, &[4u8], &[4u8]).unwrap();
    db.put(&options, &[5u8], &[5u8]).unwrap();

    db.compact(Some(&[2u8]), Some(&[4u8]));
}

#[test]
fn test_db_compact_all() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("db_compact_all").unwrap();
    let mut db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let options = WriteOptions::default();
    db.put(&options, &[1u8], &[1u8]).unwrap();
    db.put(&options, &[2u8], &[2u8]).unwrap();
    db.put(&options, &[3u8], &[3u8]).unwrap();
    db.put(&options, &[4u8], &[4u8]).unwrap();
    db.put(&options, &[5u8], &[5u8]).unwrap();

    db.compact(None, None);
}

#[test]
fn test_writebatch() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("writebatch").unwrap();
    let mut db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(&[1u8], &[1u8]);
    batch.put(&[2u8], &[2u8]);
    batch.delete(&[1u8]);

    let options = WriteOptions::default();
    let res = db.write(&options, &batch);
    assert!(res.is_ok());

    let options = ReadOptions::default();
    let res = db.get(&options, &[2u8]);
    assert!(res.is_ok());
    assert!(res.unwrap() == vec![2u8]);

    let res = db.get(&options, &[1u8]);
    assert!(res.is_ok());
    assert!(res.unwrap() == vec![]);
}

struct TesBatchWriterIter {
    put: i32,
    deleted: i32,
}

impl Default for TesBatchWriterIter {
    fn default() -> Self {
        Self {
            put: 0,
            deleted: 0,
        }
    }
}

impl WriteBatchIter for TesBatchWriterIter {
    fn put(&mut self, _key: &[u8], _value: &[u8]) {
        self.put = self.put + 1;
    }

    fn deleted(&mut self, _key: &[u8]) {
        self.deleted = self.deleted + 1;
    }
}

#[test]
fn test_writebatchiter() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("writebatchiter").unwrap();
    let db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(&[1u8], &[1u8]);
    batch.put(&[2u8], &[2u8]);
    batch.delete(&[1u8]);

    let options = WriteOptions::default();
    let res = db.write(&options, &batch);
    assert!(res.is_ok());

    let iter = batch.iterate(Box::new(TesBatchWriterIter::default()));
    assert_eq!(iter.put, 2);
    assert_eq!(iter.deleted, 1);
}

#[test]
fn test_snapshot() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("snapshot").unwrap();
    let mut db = DB::open(tmpdir.path(), Some(options)).unwrap();

    let options = WriteOptions::default();
    db.put(&options, &[1u8], &[1u8]).unwrap();
    let snapshot = db.snapshot();
    db.put(&options, &[2u8], &[2u8]).unwrap();
    let mut options = ReadOptions::default();
    options.snapshot = Some(snapshot);
    let res = db.get(&options, &[2u8]);
    assert!(res.is_ok());
    assert_eq!(vec![0u8; 0], res.unwrap());
}

#[test]
fn access_from_threads() {
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;

    let mut options = Options::default();
    options.create_if_missing = true;
    let tmpdir = TempDir::new("shared").unwrap();
    let db = DB::open(tmpdir.path(), Some(options)).unwrap();
    let shared = Arc::new(db);

    let _items = (0..10).map(|i| {
         let local_db = shared.clone();

         thread::spawn(move || {
             let options = WriteOptions::default();
             match local_db.put(&options, &[i as u8], &[i as u8]) {
                 Ok(_) => { },
                 Err(e) => { panic!("failed to write to database: {:?}", e) }
             }
         })
    })
    .map(JoinHandle::join)
    .collect::<Vec<_>>();
}