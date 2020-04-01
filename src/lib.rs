//! A database library for leveldb
//!
//! Usage:
//!
//! ```rust,ignore
//! use std::rc::Rc;
//! 
//! fn main() {
//!     let mut options = leveldb::Options::default();
//!     options.create_if_missing = true;
//!     options.filter_policy = Rc::new(Box::new(leveldb::BloomFilterPolicyV2::new(10)));
//!     let mut db = leveldb::DB::open("tempdb", Some(options)).unwrap();
//! 
//!     // add a key-value pair
//!     let options = leveldb::WriteOptions::default();
//!     db.put(&options, b"foo", b"bar").unwrap();
//! 
//!     // get the value of the created value
//!     let options = leveldb::ReadOptions::default();
//!     let value = db.get(&options, b"foo").unwrap();
//! 
//!     // iterate over the keys-value pairs
//!     println!("Value = {:?}", value);
//!     for k in db.iter(&options) {
//!         println!("item = {:?}", k);
//!     }
//! 
//!     // delete the created key
//!     let options = leveldb::WriteOptions::default();
//!     db.delete(&options, b"foo").unwrap();
//! }
//! ```

extern crate leveldb_sys;
extern crate libc;

use std::{
    cmp::Ordering,
    ffi::{CString, IntoStringError},
    fmt,
    path::Path,
    ptr,
    rc::Rc,
    iter::{Iterator},
};

use leveldb_sys::*;
use libc::{c_char, c_void, c_int, size_t};

/// A leveldb error.
pub enum Error {
    Inner(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Inner(s) => write!(f, "LevelDB error: {}", s),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::convert::From<&str> for Error {
    fn from(message: &str) -> Error {
        Self::Inner(message.to_string())
    }
}

fn errptr_to_utf8(errptr: *mut i8) -> &'static str {
    use std::ffi::CStr;
    use std::str::from_utf8;

    unsafe {
        let s = from_utf8(CStr::from_ptr(errptr).to_bytes()).unwrap();
        leveldb_free(errptr as *mut c_void);
        s
    }
}

/// An Env is an interface used by the leveldb implementation to access
/// operating system functionality like the filesystem etc.  Callers
/// may wish to provide a custom Env object when opening a database to
/// get fine gain control; e.g., to rate limit file system operations.
///
/// All Env implementations are safe for concurrent access from
/// multiple threads without any external synchronization.
pub struct Env {
    ptr: *mut leveldb_env_t,
}

impl Default for Env {
    /// Return a default environment suitable for the current operating
    /// system.  Sophisticated users may wish to provide their own Env
    /// implementation instead of relying on this default environment.
    ///
    /// The result of Default() belongs to leveldb and must never be deleted.
    fn default() -> Self {
        Self {
            ptr: unsafe { leveldb_create_default_env() },
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        unsafe { leveldb_env_destroy(self.ptr) }
    }
}

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Env({:?})", self.ptr)
    }
}

/// A trait that provides object a total order across slices that are
/// used as keys in an sstable or a database.  A Comparator implementation
/// must be thread-safe since leveldb may invoke its methods concurrently
/// from multiple threads.
pub trait Comparator {
    /// Three-way comparison.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// The name of the comparator.  Used to check for comparator
    /// mismatches (i.e., a DB created with one comparator is
    /// accessed using a different comparator.
    ///
    /// The client of this package should switch to a new name whenever
    /// the comparator implementation changes in a way that will cause
    /// the relative ordering of any two keys to change.
    ///
    /// Names starting with "leveldb." are reserved and should not be used
    /// by any clients of this package.
    fn name(&self) -> *const c_char;

    /// Retun the pointer to the internal leveldb comparator
    fn as_ptr(&self) -> *mut leveldb_comparator_t;
}

/// Internal comparator
unsafe trait IComparator: Comparator
where
    Self: Sized,
{
    extern "C" fn name(state: *mut c_void) -> *const c_char {
        let x = unsafe { &*(state as *mut Self) };
        x.name()
    }

    extern "C" fn compare(
        state: *mut c_void,
        a: *const i8,
        alen: size_t,
        b: *const i8,
        blen: size_t,
    ) -> i32 {
        unsafe {
            let a_slice = std::slice::from_raw_parts::<u8>(a as *const u8, alen as usize);
            let b_slice = std::slice::from_raw_parts::<u8>(b as *const u8, blen as usize);
            let x = &*(state as *mut Self);
            match x.compare(&a_slice, &b_slice) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        }
    }

    extern "C" fn destructor(state: *mut c_void) {
        let _x: Box<Self> = unsafe { Box::from_raw(state as *mut Self) };
    }
}

unsafe impl<C: Comparator> IComparator for C {}

fn create_comparator<C: Comparator>(c: Box<&C>) -> *mut leveldb_comparator_t {
    unsafe {
        leveldb_comparator_create(
            Box::into_raw(c) as *mut c_void,
            <C as IComparator>::destructor,
            <C as IComparator>::compare,
            <C as IComparator>::name,
        )
    }
}

/// Comparator comparing keys in reverse order.
pub struct ReverseComparator;

impl Comparator for ReverseComparator {
    fn name(&self) -> *const c_char {
        "reverse".as_ptr() as *const c_char
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
    fn as_ptr(&self) -> *mut leveldb_comparator_t {
        create_comparator(Box::new(self))
    }
}

/// An interface for writing log messages.
///
/// Note: not exposed thru the leveldb C API.
pub struct Logger {}

impl Default for Logger {
    fn default() -> Self {
        Self {}
    }
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Logger()")
    }
}

/// A Cache is an interface that maps keys to values.  It has internal
/// synchronization and may be safely accessed concurrently from
/// multiple threads.  It may automatically evict entries to make room
/// for new entries.  Values have a specified charge against the cache
/// capacity.  For example, a cache where the values are variable
/// length strings, may use the length of the string as the charge for
/// the string.
///
/// A builtin cache implementation with a least-recently-used eviction
/// policy is provided.  Clients may use their own implementations if
/// they want something more sophisticated (like scan-resistance, a
/// custom eviction policy, variable cache sizing, etc.)
pub struct Cache {
    ptr: *mut leveldb_cache_t,
}

impl Cache {
    /// Create a new cache with a fixed size capacity.  This implementation
    /// of Cache uses a least-recently-used eviction policy.
    pub fn new(size: usize) -> Cache {
        Cache {
            ptr: unsafe { leveldb_cache_create_lru(size) },
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut leveldb_cache_t {
        self.ptr
    }
}

impl Drop for Cache {
    /// Destroys all existing entries by calling the "deleter"
    /// function that was passed to the constructor.
    fn drop(&mut self) {
        unsafe { leveldb_cache_destroy(self.ptr) }
    }
}

impl fmt::Debug for Cache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cache({:?})", self.ptr)
    }
}

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
pub enum CompressionType {
    None,
    Snappy,
}

impl fmt::Debug for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "NoCompression"),
            Self::Snappy => write!(f, "SnappyCompression"),
        }
    }
}

// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support.
pub trait FilterPolicy {
    fn name(&self) -> String;
    fn as_ptr(&self) -> *mut leveldb_filterpolicy_t;
}

pub struct NoFilterPolicy;

impl FilterPolicy for NoFilterPolicy {
    fn name(&self) -> String {
        String::from("leveldb.NoFilter")
    }
    fn as_ptr(&self) -> *mut leveldb_filterpolicy_t {
        ptr::null_mut() as *mut leveldb_filterpolicy_t
    }
}

impl fmt::Debug for NoFilterPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NoFilterPolicy")
    }
}

pub struct BloomFilterPolicyV2 {
    ptr: *mut leveldb_filterpolicy_t,
    bits_per_key: c_int,
}

impl BloomFilterPolicyV2 {
    pub(crate) const NAME: &'static str = "leveldb.BuiltinBloomFilter2";

    pub fn new(bits_per_key: i32) -> Self {
        Self {
            ptr: unsafe { leveldb_filterpolicy_create_bloom(bits_per_key as c_int) },
            bits_per_key: bits_per_key as c_int,
        }
    }
}

impl FilterPolicy for BloomFilterPolicyV2 {
    fn name(&self) -> String {
        String::from(Self::NAME)
    }
    fn as_ptr(&self) -> *mut leveldb_filterpolicy_t {
        ptr::null_mut() as *mut leveldb_filterpolicy_t
    }
}

impl fmt::Debug for BloomFilterPolicyV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BloomFilterPolicyV2({:?}, {})", self.ptr, self.bits_per_key)
    }
}

impl Drop for BloomFilterPolicyV2 {
    fn drop(&mut self) {
        unsafe { leveldb_filterpolicy_destroy(self.ptr) }
    }
}

/// Abstract handle to particular state of a DB.
/// A Snapshot is an immutable object and can therefore be safely
/// accessed from multiple threads without any external synchronization.
///
/// Snapshots are kept in a doubly-linked list in the DB.
/// Each SnapshotImpl corresponds to a particular sequence number.
#[derive(Clone)]
pub struct Snapshot {
    db_ptr: *mut leveldb_t,
    ptr: *mut leveldb_snapshot_t,
}

impl Snapshot {
    pub(crate) fn as_ptr(&self) -> *mut leveldb_snapshot_t {
        self.ptr
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        unsafe { leveldb_release_snapshot(self.db_ptr, self.ptr) };
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Snapshot({:?}, {:?})", self.ptr, self.db_ptr)
    }
}

/// Options to control the behavior of a database (passed to DB::open)
pub struct Options {
    // -------------------
    // Parameters that affect behavior
    /// Comparator used to define the order of keys in the table.
    /// Default: a comparator that uses lexicographic byte-wise ordering
    ///
    /// REQUIRES: The client must ensure that the comparator supplied
    /// here has the same name and orders keys *exactly* the same as the
    /// comparator provided to previous open calls on the same DB.
    pub comparator: Option<Box<dyn Comparator>>,

    /// If true, the database will be created if it is missing.
    pub create_if_missing: bool,

    /// If true, an error is raised if the database already exists.
    pub error_if_exists: bool,

    /// If true, the implementation will do aggressive checking of the
    /// data it is processing and will stop early if it detects any
    /// errors.  This may have unforeseen ramifications: for example, a
    /// corruption of one DB entry may cause a large number of entries to
    /// become unreadable or for the entire DB to become unopenable.
    pub paranoid_checks: bool,

    /// Use the specified object to interact with the environment,
    /// e.g. to read/write files, schedule background work, etc.
    /// Default: Env::default()
    pub env: Env,

    /// Any internal progress/error information generated by the db will
    /// be written to info_log if it is non-null, or to a file stored
    /// in the same directory as the DB contents if info_log is null.
    pub info_log: Option<Logger>,

    // -------------------
    // Parameters that affect performance
    /// Amount of data to build up in memory (backed by an unsorted log
    /// on disk) before converting to a sorted on-disk file.
    ///
    /// Larger values increase performance, especially during bulk loads.
    /// Up to two write buffers may be held in memory at the same time,
    /// so you may wish to adjust this parameter to control memory usage.
    /// Also, a larger write buffer will result in a longer recovery time
    /// the next time the database is opened.
    pub write_buffer_size: usize,

    /// Number of open files that can be used by the DB.  You may need to
    /// increase this if your database has a large working set (budget
    /// one open file per 2MB of working set).
    pub max_open_files: i32,

    // Control over blocks (user data is stored in a set of blocks, and
    // a block is the unit of reading from disk).
    /// If non-null, use the specified cache for blocks.
    /// If null, leveldb will automatically create and use an 8MB internal cache.
    pub block_cache: Option<Cache>,

    /// Approximate size of user data packed per block.  Note that the
    /// block size specified here corresponds to uncompressed data.  The
    /// actual size of the unit read from disk may be smaller if
    /// compression is enabled.  This parameter can be changed dynamically.
    pub block_size: usize,

    /// Number of keys between restart points for delta encoding of keys.
    /// This parameter can be changed dynamically.  Most clients should
    /// leave this parameter alone.
    pub block_restart_interval: i32,

    /// Leveldb will write up to this amount of bytes to a file before
    /// switching to a new one.
    /// Most clients should leave this parameter alone.  However if your
    /// filesystem is more efficient with larger files, you could
    /// consider increasing the value.  The downside will be longer
    /// compactions and hence longer latency/performance hiccups.
    /// Another reason to increase this parameter might be when you are
    /// initially populating a large database.
    pub max_file_size: usize,

    /// Compress blocks using the specified compression algorithm.  This
    /// parameter can be changed dynamically.
    ///
    /// Default: kSnappyCompression, which gives lightweight but fast
    /// compression.
    ///
    /// Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
    ///    ~200-500MB/s compression
    ///    ~400-800MB/s decompression
    /// Note that these speeds are significantly faster than most
    /// persistent storage speeds, and therefore it is typically never
    /// worth switching to kNoCompression.  Even if the input data is
    /// incompressible, the kSnappyCompression implementation will
    /// efficiently detect that and will switch to uncompressed mode.
    pub compression: CompressionType,

    /// EXPERIMENTAL: If true, append to existing MANIFEST and log files
    /// when a database is opened.  This can significantly speed up open.
    ///
    /// Default: currently false, but may become true later.
    pub reuse_logs: bool,

    /// If non-null, use the specified filter policy to reduce disk reads.
    /// Many applications will benefit from passing the result of
    /// Box::new(BloomFilterPolicy::new()) here.
    pub filter_policy: Rc<Box<dyn FilterPolicy>>,
}

impl Options {
    pub(crate) unsafe fn as_ptr(&mut self) -> *mut leveldb_options_t {
        let o = leveldb_options_create();
        leveldb_options_set_create_if_missing(o, self.create_if_missing as u8);
        leveldb_options_set_error_if_exists(o, self.error_if_exists as u8);
        leveldb_options_set_paranoid_checks(o, self.paranoid_checks as u8);
        leveldb_options_set_write_buffer_size(o, self.write_buffer_size);
        leveldb_options_set_max_open_files(o, self.max_open_files);
        leveldb_options_set_block_size(o, self.block_size);
        leveldb_options_set_block_restart_interval(o, self.block_restart_interval);
        leveldb_options_set_compression(
            o,
            match self.compression {
                CompressionType::None => Compression::No,
                CompressionType::Snappy => Compression::Snappy,
            },
        );
        if let Some(comparator) = &self.comparator {
            leveldb_options_set_comparator(o, comparator.as_ptr());
        }
        if let Some(ref cache) = self.block_cache {
            leveldb_options_set_cache(o, cache.as_ptr());
        }
        if BloomFilterPolicyV2::NAME == self.filter_policy.name() {
            leveldb_options_set_filter_policy(o, self.filter_policy.as_ptr());
        }
        o
    }
}

impl Default for Options {
    /// Create an Options object with default values for all fields.
    fn default() -> Self {
        Self {
            comparator: None,
            create_if_missing: false,
            error_if_exists: false,
            paranoid_checks: false,
            env: Env::default(),
            info_log: None,
            write_buffer_size: 4 * 1024 * 1024,
            max_open_files: 1000,
            block_cache: None,
            block_size: 4 * 1024,
            block_restart_interval: 16,
            max_file_size: 2 * 1024 * 1024,
            compression: CompressionType::Snappy,
            reuse_logs: false,
            filter_policy: Rc::new(Box::new(NoFilterPolicy)),
        }
    }
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            // TODO(vimo) add comparator
            .field("create_if_missing", &self.create_if_missing)
            .field("error_if_exists", &self.error_if_exists)
            .field("paranoid_checks", &self.paranoid_checks)
            // TODO(vimo) add env & info_log
            .field("write_buffer_size", &self.write_buffer_size)
            .field("max_open_files", &self.max_open_files)
            .field("block_cache", &self.block_cache)
            .field("block_size", &self.block_size)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("max_file_size", &self.max_file_size)
            .field("compression", &self.compression)
            .field("reuse_logs", &self.reuse_logs)
            // TODO(vimo) add filter policy
            .finish()
    }
}

/// Options that control read operations
#[derive(Clone)]
pub struct ReadOptions {
    /// If true, all data read from underlying storage will be
    /// verified against corresponding checksums.
    pub verify_checksums: bool,

    /// Should the data read for this iteration be cached in memory?
    /// Callers may wish to set this field to false for bulk scans.
    pub fill_cache: bool,

    /// If "snapshot" is non-null, read as of the supplied snapshot
    /// (which must belong to the DB that is being read and which must
    /// not have been released).  If "snapshot" is null, use an implicit
    /// snapshot of the state at the beginning of this read operation.
    pub snapshot: Option<Snapshot>,
}

impl ReadOptions {
    pub(crate) unsafe fn as_ptr(&self) -> *mut leveldb_readoptions_t {
        let o = leveldb_readoptions_create();
        leveldb_readoptions_set_verify_checksums(o, self.verify_checksums as u8);
        leveldb_readoptions_set_fill_cache(o, self.fill_cache as u8);
        if let Some(ref snapshot) = self.snapshot {
            leveldb_readoptions_set_snapshot(o, snapshot.as_ptr());
        }
        o
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            verify_checksums: false,
            fill_cache: true,
            snapshot: None,
        }
    }
}

impl fmt::Debug for ReadOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadOptions")
            .field("verify_checksums", &self.verify_checksums)
            .field("fill_cache", &self.fill_cache)
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

pub struct WriteOptions {
    /// If true, the write will be flushed from the operating system
    /// buffer cache (by calling WritableFile::Sync()) before the write
    /// is considered complete.  If this flag is true, writes will be
    /// slower.
    ///
    /// If this flag is false, and the machine crashes, some recent
    /// writes may be lost.  Note that if it is just the process that
    /// crashes (i.e., the machine does not reboot), no writes will be
    /// lost even if sync==false.
    ///
    /// In other words, a DB write with sync==false has similar
    /// crash semantics as the "write()" system call.  A DB write
    /// with sync==true has similar crash semantics to a "write()"
    /// system call followed by "fsync()".
    pub sync: bool,
}

impl WriteOptions {
    pub(crate) unsafe fn as_ptr(&self) -> *mut leveldb_writeoptions_t {
        let o = leveldb_writeoptions_create();
        leveldb_writeoptions_set_sync(o, self.sync as u8);
        o
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { sync: false }
    }
}

impl fmt::Debug for WriteOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteOptions")
            .field("sync", &self.sync)
            .finish()
    }
}

/// Bytes allocated by leveldb
///
/// It's basically the same thing as `Box<[u8]>` except that it uses
/// leveldb_free() as a destructor.
///
/// [source](https://github.com/skade/leveldb/blob/master/src/database/bytes.rs)
struct Bytes {
    // We use static reference instead of pointer to inform the compiler that
    // it can't be null. (Because `NonZero` is unstable now.)
    bytes: &'static mut u8,
    size: usize,
    // Tells the compiler that we own u8
    marker: std::marker::PhantomData<u8>,
}

impl Bytes {
    /// Creates instance of `Bytes` from leveldb-allocated data.
    ///
    /// Returns `None` if `ptr` is `null`.
    pub fn from_raw(ptr: *mut u8, size: usize) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            Some(unsafe {
                Self {
                    bytes: &mut *ptr,
                    size,
                    marker: Default::default(),
                }
            })
        }
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        unsafe {
            leveldb_sys::leveldb_free(self.bytes as *mut u8 as *mut c_void);
        }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.bytes, self.size) }
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.bytes as *mut u8, self.size) }
    }
}

impl std::borrow::Borrow<[u8]> for Bytes {
    fn borrow(&self) -> &[u8] {
        &*self
    }
}

impl std::borrow::BorrowMut<[u8]> for Bytes {
    fn borrow_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

impl AsMut<[u8]> for Bytes {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut *self
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Self {
        bytes.as_ref().to_owned()
    }
}

impl From<Bytes> for Box<[u8]> {
    fn from(bytes: Bytes) -> Self {
        bytes.as_ref().to_owned().into_boxed_slice()
    }
}

/// WriteBatch holds a collection of updates to apply atomically to a DB.
///
/// The updates are applied in the order in which they are added
/// to the WriteBatch.  For example, the value of "key" will be "v3"
/// after the following batch is written:
///
///    batch.put(b"key", b"v1");
///    batch.delete(b"key");
///    batch.put(b"key", b"v2");
///    batch.put(b"key", b"v3");
///
/// Multiple threads can invoke const methods on a WriteBatch without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same WriteBatch must use
/// external synchronization.
pub struct WriteBatch {
    ptr: *mut leveldb_writebatch_t,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    /// Creates a new WriteBatch
    pub fn new() -> Self {
        Self {
            ptr: unsafe { leveldb_writebatch_create() },
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut leveldb_writebatch_t {
        self.ptr
    }

    /// Store the mapping "key->value" in the database.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        unsafe {
            leveldb_writebatch_put(
                self.ptr,
                key.as_ptr() as *mut c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            );
        }
    }

    /// If the database contains a mapping for "key", erase it.  Else do nothing.
    pub fn delete(&mut self, key: &[u8]) {
        unsafe {
            leveldb_writebatch_delete(self.ptr, key.as_ptr() as *mut c_char, key.len());
        }
    }

    /// Clear all updates buffered in this batch.
    pub fn clear(&mut self) {
        unsafe { leveldb_writebatch_clear(self.ptr) };
    }

    /// Support for iterating over the contents of a batch.
    pub fn iterate<T: WriteBatchIter>(&mut self, iterator: Box<T>) -> Box<T> {
        let state = Box::into_raw(iterator);
        unsafe {
            leveldb_writebatch_iterate(
                self.ptr,
                state as *mut c_void,
                writebatch_put_callback::<T>,
                writebatch_deleted_callback::<T>,
            );
            Box::from_raw(state)
        }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { leveldb_writebatch_destroy(self.ptr) }
    }
}

impl fmt::Debug for WriteBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WriteBatch({:?})", self.ptr)
    }
}

/// A trait implemented by object that can iterator over written batches and
/// check their validity.
pub trait WriteBatchIter {
    /// Callback for put items
    fn put(&mut self, key: &[u8], value: &[u8]);

    /// Callback for deleted items
    fn deleted(&mut self, key: &[u8]);
}

extern "C" fn writebatch_put_callback<T: WriteBatchIter>(
    state: *mut c_void,
    key: *const i8,
    keylen: size_t,
    val: *const i8,
    vallen: size_t,
) {
    unsafe {
        let iter: &mut T = &mut *(state as *mut T);
        let key_slice = std::slice::from_raw_parts::<u8>(key as *const u8, keylen as usize);
        let val_slice = std::slice::from_raw_parts::<u8>(val as *const u8, vallen as usize);
        iter.put(key_slice, val_slice);
    }
}

extern "C" fn writebatch_deleted_callback<T: WriteBatchIter>(
    state: *mut c_void,
    key: *const i8,
    keylen: size_t,
) {
    unsafe {
        let iter: &mut T = &mut *(state as *mut T);
        let key_slice = std::slice::from_raw_parts::<u8>(key as *const u8, keylen as usize);
        iter.deleted(key_slice);
    }
}

/// An iterator yields a sequence of key/value pairs from a source.
/// The following class defines the interface.  Multiple implementations
/// are provided by this library.  In particular, iterators are provided
/// to access the contents of a Table or a DB.
///
/// Multiple threads can invoke const methods on an Iterator without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same Iterator must use
/// external synchronization.
pub struct Iter {
    ptr: *mut leveldb_iterator_t,
    start: bool,
    from: Vec<u8>,
}

impl Iter {
    /// Creates a new iterator for given DB and options.
    pub(crate) fn new(db: &DB, options: &ReadOptions) -> Self {
        Self {
            ptr: unsafe {
                let o = options.as_ptr();
                let ptr = leveldb_create_iterator(db.ptr, o);
                leveldb_readoptions_destroy(o);
                leveldb_iter_seek_to_first(ptr);
                ptr
            },
            start: true,
            from: vec![],
        }
    }

    /// An iterator is either positioned at a key/value pair, or
    /// not valid.  This method returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        unsafe { leveldb_iter_valid(self.ptr) != 0 }
    }

    /// Position at the first key in the source.  The iterator is Valid()
    /// after this call iff the source is not empty.
    pub fn seek_to_first(&mut self) {
        unsafe { leveldb_iter_seek_to_first(self.ptr) }
    }

    /// Position at the last key in the source.  The iterator is
    /// Valid() after this call iff the source is not empty.
    pub fn seek_to_last(&mut self) {
        unsafe { leveldb_iter_seek_to_last(self.ptr) }
    }

    /// Position at the first key in the source that is at or past target.
    /// The iterator is Valid() after this call iff the source contains
    /// an entry that comes at or past target.
    pub fn seek(&mut self, target: &[u8]) {
        unsafe { leveldb_iter_seek(self.ptr, target.as_ptr() as *mut c_char, target.len()) }
    }

    /// Moves to the previous entry in the source.  After this call, Valid() is
    /// true iff the iterator was not positioned at the first entry in source.
    /// REQUIRES: Valid()
    pub fn prev(&mut self) {}

    /// Return the key for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    pub fn key(&self) -> Vec<u8> {
        unsafe {
            let len: usize = 0;
            let value = leveldb_iter_value(self.ptr, &len) as *const u8;
            std::slice::from_raw_parts(value, len)
        }
        .to_vec()
    }

    /// Return the value for the current entry.  The underlying storage for
    /// the returned slice is valid only until the next modification of
    /// the iterator.
    pub fn value(&self) -> Vec<u8> {
        unsafe {
            let len: usize = 0;
            let value = leveldb_iter_key(self.ptr, &len) as *const u8;
            std::slice::from_raw_parts(value, len)
        }
        .to_vec()
    }
}

impl Iterator for Iter {
    type Item = (Vec<u8>, Vec<u8>);

    /// Moves to the next entry in the source.  After this call, Valid() is
    /// true iff the iterator was not positioned at the last entry in the source.
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        unsafe {
            if !self.start {
                leveldb_iter_next(self.ptr);
            } else {
                if !self.from.is_empty() {
                    let target = self.from.to_owned();
                    self.seek(target.as_slice());
                }
                self.start = false;
            }
        }
        if self.is_valid() {
            Some((self.key(), self.value()))
        } else {
            None
        }
    }
}

impl Drop for Iter {
    fn drop(&mut self) {
        unsafe { leveldb_iter_destroy(self.ptr) }
    }
}

impl fmt::Debug for Iter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Iterator({:?}) start={:?}, from={:?}",
            self.ptr, self.start, self.from
        )
    }
}

/// A DB is a persistent ordered map from keys to values.
///
/// A DB is safe for concurrent access from multiple threads without
/// any external synchronization.
pub struct DB {
    ptr: *mut leveldb_t,
}

impl DB {
    /// The leveldb major version.
    pub fn major_version() -> usize {
        unsafe { leveldb_major_version() as usize }
    }

    /// The leveldb minor version.
    pub fn minor_version() -> usize {
        unsafe { leveldb_minor_version() as usize }
    }

    /// Create a new database from raw pointer.
    fn new(ptr: *mut leveldb_t) -> Self {
        Self { ptr }
    }

    /// Open the database with the specified "name".
    /// Stores a pointer to a heap-allocated database in *dbptr and returns
    /// OK on success.
    /// Stores nullptr in *dbptr and returns a non-OK status on error.
    /// Caller should delete *dbptr when it is no longer needed.
    pub fn open<P: AsRef<Path>>(name: P, options: Option<Options>) -> Result<DB, Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let name = CString::new(name.as_ref().to_str().unwrap()).unwrap();
            let o = match options {
                Some(mut opts) => opts.as_ptr(),
                None => Options::default().as_ptr(),
            };
            let db = leveldb_open(
                o,
                name.as_bytes_with_nul().as_ptr() as *const i8,
                &mut errptr,
            );
            leveldb_options_destroy(o);
            if errptr.is_null() {
                Ok(DB::new(db))
            } else {
                Err(Error::from(errptr_to_utf8(errptr)))
            }
        }
    }

    /// Destroy the contents of the specified database.
    /// Be very careful using this method.
    ///
    /// Note: For backwards compatibility, if DestroyDB is unable to list the
    /// database files, Status::OK() will still be returned masking this failure.
    pub fn destroy<P: AsRef<Path>>(name: P, options: &mut Options) -> Result<(), Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let name = CString::new(name.as_ref().to_str().unwrap()).unwrap();
            let o = options.as_ptr();
            leveldb_destroy_db(
                o,
                name.as_bytes_with_nul().as_ptr() as *const c_char,
                &mut errptr,
            );
            leveldb_options_destroy(o);
        }
        if errptr.is_null() {
            Ok(())
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// If a DB cannot be opened, you may attempt to call this method to
    /// resurrect as much of the contents of the database as possible.
    /// Some data may be lost, so be careful when calling this function
    /// on a database that contains important information.
    pub fn repair<P: AsRef<Path>>(name: P, options: &mut Options) -> Result<(), Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let name = CString::new(name.as_ref().to_str().unwrap()).unwrap();
            let o = options.as_ptr();
            leveldb_repair_db(
                o,
                name.as_bytes_with_nul().as_ptr() as *const c_char,
                &mut errptr,
            );
            leveldb_options_destroy(o);
        }
        if errptr.is_null() {
            Ok(())
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// Set the database entry for "key" to "value".  Returns OK on success,
    /// and a non-OK status on error.
    /// Note: consider setting options.sync = true.
    pub fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let o = options.as_ptr();
            leveldb_put(
                self.ptr,
                o,
                key.as_ptr() as *mut c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
                &mut errptr,
            );
            leveldb_writeoptions_destroy(o);
        }
        if errptr.is_null() {
            Ok(())
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// Remove the database entry (if any) for "key".  Returns OK on
    /// success, and a non-OK status on error.  It is not an error if "key"
    /// did not exist in the database.
    /// Note: consider setting options.sync = true.
    pub fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<(), Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let o = options.as_ptr();
            leveldb_delete(
                self.ptr,
                o,
                key.as_ptr() as *mut c_char,
                key.len(),
                &mut errptr,
            );
            leveldb_writeoptions_destroy(o);
        }
        if errptr.is_null() {
            Ok(())
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// Apply the specified updates to the database.
    /// Returns OK on success, non-OK on failure.
    /// Note: consider setting options.sync = true.
    pub fn write(&self, options: &WriteOptions, updates: &WriteBatch) -> Result<(), Error> {
        let mut errptr = ptr::null_mut();
        unsafe {
            let o = options.as_ptr();
            leveldb_write(self.ptr, o, updates.as_ptr(), &mut errptr);
            leveldb_writeoptions_destroy(o);
        }
        if errptr.is_null() {
            Ok(())
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// If the database contains an entry for "key" store the
    /// corresponding value in *value and return OK.
    ///
    /// If there is no entry for "key" leave *value unchanged and return
    /// a status for which Status::IsNotFound() returns true.
    ///
    /// May return some other Status on an error.
    pub fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Vec<u8>, Error> {
        let mut errptr = ptr::null_mut();
        let mut vallen: usize = 0;
        let result = unsafe {
            let o = options.as_ptr();
            let result = leveldb_get(
                self.ptr,
                o,
                key.as_ptr() as *mut c_char,
                key.len(),
                &mut vallen,
                &mut errptr,
            );
            leveldb_readoptions_destroy(o);
            result as *mut u8
        };
        if errptr.is_null() {
            Ok(match Bytes::from_raw(result, vallen) {
                Some(bytes) => bytes.to_vec(),
                None => vec![],
            })
        } else {
            Err(Error::from(errptr_to_utf8(errptr)))
        }
    }

    /// Return a heap-allocated iterator over the contents of the database.
    /// The result of NewIterator() is initially invalid (caller must
    /// call one of the Seek methods on the iterator before using it).
    ///
    /// Caller should delete the iterator when it is no longer needed.
    /// The returned iterator should be deleted before this db is deleted.
    pub fn iter(&self, options: &ReadOptions) -> Iter {
        Iter::new(self, options)
    }

    /// Return a handle to the current DB state.  Iterators created with
    /// this handle will all observe a stable snapshot of the current DB
    /// state.  The caller must call ReleaseSnapshot(result) when the
    /// snapshot is no longer needed.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            ptr: unsafe { leveldb_create_snapshot(self.ptr) },
            db_ptr: self.ptr,
        }
    }

    /// DB implementations can export properties about their state
    /// via this method.  If "property" is a valid property understood by this
    /// DB implementation, fills "*value" with its current value and returns
    /// true.  Otherwise returns false.
    ///
    ///
    /// Valid property names include:
    ///
    ///  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
    ///     where <N> is an ASCII representation of a level number (e.g. "0").
    ///  "leveldb.stats" - returns a multi-line string that describes statistics
    ///     about the internal operation of the DB.
    ///  "leveldb.sstables" - returns a multi-line string that describes all
    ///     of the sstables that make up the db contents.
    ///  "leveldb.approximate-memory-usage" - returns the approximate number of
    ///     bytes of memory in use by the DB.
    pub fn get_property_value(&self, propname: String) -> Result<String, IntoStringError> {
        let propname = CString::new(propname.as_str()).unwrap();
        unsafe {
            CString::from_raw(leveldb_property_value(
                self.ptr,
                propname.as_bytes_with_nul().as_ptr() as *const c_char,
            ))
        }
        .into_string()
    }

    /// Compact the underlying storage for the key range [*start,*end].
    /// In particular, deleted and overwritten versions are discarded,
    /// and the data is rearranged to reduce the cost of operations
    /// needed to access the data.  This operation should typically only
    /// be invoked by users who understand the underlying implementation.
    ///
    /// start==None is treated as a key before all keys in the database.
    /// end==None is treated as a key after all keys in the database.
    /// Therefore the following call will compact the entire database:
    ///    db.compact_range(None, None);
    pub fn compact(&mut self, start: Option<&[u8]>, end: Option<&[u8]>) {
        let (start_key, start_key_len) = parse_nullable_slice_parts(start);
        let (end_key, end_key_len) = parse_nullable_slice_parts(end);
        unsafe {
            leveldb_compact_range(
                self.ptr,
                start_key as *mut c_char,
                start_key_len,
                end_key as *mut c_char,
                end_key_len,
            )
        }
    }
}

fn parse_nullable_slice_parts(key: Option<&[u8]>) -> (*const u8, usize) {
    match key {
        Some(key) => (key.as_ptr(), key.len()),
        None => (ptr::null(), 0),
    }
}

unsafe impl Sync for DB {}
unsafe impl Send for DB {}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DB({:?})", self.ptr)
    }
}
