//! # MerkleStorage
//!
//! Storage for key/values with git-like semantics and history.
//!
//! # Data Structure
//! A storage with just one key `a/b/c` and its corresponding value `8` is represented like this:
//!
//! ``
//! [commit] ---> [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//! ``
//!
//! The db then contains the following:
//! ```no_compile
//! <hash_of_blob; blob8>
//! <hash_of_tree3, tree3>, where tree3 is a map {c: hash_blob8}
//! <hash_of_tree2, tree2>, where tree2 is a map {b: hash_of_tree3}
//! <hash_of_tree2, tree2>, where tree1 is a map {a: hash_of_tree2}
//! <hash_of_commit>; commit>, where commit points to the root tree (tree1)
//! ```
//!
//! Then, when looking for a path a/b/c in a spcific commit, we first get the hash of the root tree
//! from the commit, then get the tree from the database, get the hash of "a", look it up in the db,
//! get the hash of "b" from that tree, load from db, then get the hash of "c" and retrieve the
//! final value.
//!
//!
//! Now, let's assume we want to add a path `X` also referencing the value `8`. That creates a new
//! tree that reuses the previous subtree for `a/b/c` and branches away from root for `X`:
//!
//! ```no_compile
//! [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//!                   ^                             ^
//!                   |                             |
//! [tree_X]----a-----                              |
//!     |                                           |
//!      ----------------------X--------------------
//! ```
//!
//! The following is added to the database:
//! ``
//! <hash_of_tree_X; tree_X>, where tree_X is a map {a: hash_of_tree2, X: hash_of_blob8}
//! ``
//!
//! Reference: https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
use rocksdb::{ColumnFamilyDescriptor, WriteBatch, Cache};
use crate::persistent::{default_table_options, KeyValueSchema, KeyValueStoreWithSchema};
use crate::persistent::database::RocksDBStats;
use crate::persistent;
use std::hash::Hash;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use im::OrdMap;
use failure::Fail;
use std::sync::Arc;
use std::time::Instant;
use crypto::hash::HashType;
use std::convert::TryInto;
use crate::persistent::BincodeEncoded;
use sled;
use std::cell::RefCell;

use sodiumoxide::crypto::generichash::State;

const HASH_LEN: usize = 32;

pub type ContextKey = Vec<String>;
pub type ContextValue = Vec<u8>;
pub type EntryHash = [u8; HASH_LEN];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum NodeKind {
    NonLeaf,
    Leaf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Node {
    node_kind: NodeKind,
    entry_hash: EntryHash,
}

pub type Tree = OrdMap<String, Node>;

#[derive(Debug, Hash, Clone, Serialize, Deserialize,PartialEq, Eq)]
pub struct Commit {
    parent_commit_hash: Option<EntryHash>,
    root_hash: EntryHash,
    time: u64,
    author: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Commit),
}

pub type MerkleStorageKV = dyn KeyValueStoreWithSchema<MerkleStorageSchema> + Sync + Send;



#[derive(Debug, Fail)]
pub enum MerkleError {
    /// External libs errors
    #[fail(display = "RocksDB error: {:?}", error)]
    DBError { error: persistent::database::DBError },
    #[fail(display = "Serialization error: {:?}", error)]
    SerializationError { error: bincode::Error },

    /// Internal unrecoverable bugs that should never occur
    #[fail(display = "No root retrieved for this commit!")]
    CommitRootNotFound,
    #[fail(display = "Cannot commit without a predecessor!")]
    MissingAncestorCommit,
    #[fail(display = "There is a commit or three under key {:?}, but not a value!", key)]
    ValueIsNotABlob { key: String },
    #[fail(display = "Found wrong structure. Was looking for {}, but found {}", sought, found)]
    FoundUnexpectedStructure { sought: String, found: String },
    #[fail(display = "Entry not found! Hash={}", hash)]
    EntryNotFound { hash: String },

    /// Wrong user input errors
    #[fail(display = "No value under key {:?}.", key)]
    ValueNotFound { key: String },
    #[fail(display = "Cannot search for an empty key.")]
    KeyEmpty,
}

impl From<persistent::database::DBError> for MerkleError {
    fn from(error: persistent::database::DBError) -> Self { MerkleError::DBError { error } }
}

impl From<bincode::Error> for MerkleError {
    fn from(error: bincode::Error) -> Self { MerkleError::SerializationError { error } }
}

#[derive(Serialize, Debug, Clone, Copy)]
pub struct MerkleMapStats {
    staged_area_elems: u64,
    current_tree_elems: u64,
}

#[derive(Serialize, Debug, Clone, Copy)]
pub struct MerklePerfStats {
    pub avg_set_exec_time_ns: f64,
}

#[derive(Serialize, Debug, Clone)]
pub struct MerkleStorageStats {
    rocksdb_stats: RocksDBStats,
    map_stats: MerkleMapStats,
    pub perf_stats: MerklePerfStats,
}

impl BincodeEncoded for EntryHash {} 


pub type WriteTransaction = Vec<(EntryHash, Entry)>;


pub trait MerkleStorageStorageBackend {

    fn create_transaction(&self) -> WriteTransaction;

    fn execute_transaction(& self, t: WriteTransaction) -> Result<(),MerkleError>;

    fn get_value(&self,key: &EntryHash) -> Option<Entry>;
}

pub struct MerkleStorageSchema{

}

impl KeyValueSchema for MerkleStorageSchema {
    type Key = EntryHash;
    type Value = Vec<u8>;

    fn descriptor(cache: &Cache) -> ColumnFamilyDescriptor {
        let cf_opts = default_table_options(cache);
        ColumnFamilyDescriptor::new(Self::name(), cf_opts)
    }

    #[inline]
    fn name() -> &'static str {
        "merkle_storage"
    }
}

impl KeyValueSchema for MerkleStorage {
    type Key = EntryHash;
    type Value = Vec<u8>;

    fn descriptor(cache: &Cache) -> ColumnFamilyDescriptor {
        let cf_opts = default_table_options(cache);
        ColumnFamilyDescriptor::new(Self::name(), cf_opts)
    }

    #[inline]
    fn name() -> &'static str {
        "merkle_storage"
    }
}

pub struct InMemoryBackend {
    // all operations on RocketDb(KeyValueStoreWithSchema) backend does not require
    // to use mutable reference - to fit into that scenario RefCell is used to allow
    // modifications of in_memory_data on immutable reference to KeyValueStore
    in_memory_data: RefCell<HashMap<EntryHash, Entry>>,
    persistent_storage: sled::Db,
}

// common interface for old rocketdb backend and new in memory implementation
impl MerkleStorageStorageBackend for InMemoryBackend {

    fn create_transaction(&self) -> WriteTransaction {
        return WriteTransaction::new();
    }

    fn execute_transaction(& self, transaction: WriteTransaction) -> Result<(),MerkleError>{
        //probably needs a mutex but we are using ARC only to fit into existing interface but still ....
        for t in transaction.iter() {
            let x = match t.1 {
                Entry::Tree(_) => { String::from("tree") },
                Entry::Blob(_) => { String::from("blob") },
                Entry::Commit(_) => { String::from("commit") },
            };

            match t.1.clone() {
                Entry::Tree(_) => { self.in_memory_data.borrow_mut().insert(t.0, t.1.clone()); }
                Entry::Commit(_) => { self.in_memory_data.borrow_mut().insert(t.0, t.1.clone()); }
                Entry::Blob(_) => { self.in_memory_data.borrow_mut().insert(t.0, t.1.clone()); }
            }
            println!("executing {} => {:?}", x, t.0);
        }
        return Ok(());
    }

    fn get_value(&self,key: &EntryHash) -> Option<Entry>{
        return match self.in_memory_data.borrow().get(key).map(|e| e.clone()){
            Some(entry) => {Some(entry)},
            None => {
                return self.persistent_storage.get(key)
                    .map_or(None,|v|
                        v.map(|option| Entry::Blob(option.to_vec()))
                    )
            }
        }
    }
}
//backward compatible implementation
impl MerkleStorageStorageBackend for Arc<MerkleStorageKV> {
    fn create_transaction(&self) -> WriteTransaction {
        return WriteTransaction::new();
    }

    fn execute_transaction(& self, transaction: WriteTransaction) -> Result<(),MerkleError> {
        let mut batch = WriteBatch::default();

        for t in transaction.iter() {
            let payload = &bincode::serialize(&t.1).unwrap();
            self.put_batch(
                &mut batch,
                &t.0,
                payload
                )?;
        }

        self.write_batch(batch)?;
        Ok(())
    }

    fn get_value(&self, key: &EntryHash) -> Option<Entry> {
        let entry_bytes = self.get(key).ok().unwrap();
        match entry_bytes{
            None => {None}
            Some(bytes) => {
                return Some(bincode::deserialize(&bytes).unwrap());
            }
        }
    }
}



impl InMemoryBackend {

    pub fn new(path: &str) -> InMemoryBackend {
        println!("opening database '{}'", path);
        let db = sled::Config::default().path(path).open().unwrap();
        let historic_data = db.get(InMemoryBackend::get_in_memory_data_tag());
        let in_memory_db: RefCell<HashMap<EntryHash, Entry>> = match historic_data {
            Ok(data) => {
                data.map_or(RefCell::new(HashMap::new()), |v| bincode::deserialize(v.to_vec().as_slice()).unwrap())
            },
            // handle db open
            _ => {
                RefCell::new(HashMap::new())
            }
        };
        println!("{} entries read from persistent storage", in_memory_db.borrow().len());

        InMemoryBackend {
            in_memory_data: in_memory_db,
            // TODO looks like sled has supprot for compression - could be useful at some point
            persistent_storage: db
        }
    }

    #[inline]
    pub fn get_in_memory_data_tag() -> &'static str {
        return "in_memory_data";
    }

    #[inline]
    pub fn get_db_path() -> &'static str {
        return "/tmp/db.sled";
    }


}

impl Drop for InMemoryBackend {
    fn drop(&mut self) {
        let payload : Vec<u8> = bincode::serialize(&self.in_memory_data).unwrap();
        self.persistent_storage.insert(InMemoryBackend::get_in_memory_data_tag(), payload).unwrap();
        println!("{} saved to database {}", self.in_memory_data.borrow().len(), InMemoryBackend::get_db_path());
        // check playground loooks like write can be implemented as "merge"
        // self.persistent_storage.insert()
    }
}


pub struct MerkleStorageGeneric<Backend: MerkleStorageStorageBackend> {
    current_stage_tree: Option<Tree>,
    db: Backend,
    staged: HashMap<EntryHash, Entry>,
    last_commit: Option<Commit>,
    map_stats: MerkleMapStats,
    cumul_set_exec_time: f64, // divide this by the next field to get avg time spent in _set
    set_exec_times: u64,
    set_exec_times_to_discard: u64, // first N measurements to discard
}

impl<Backend: MerkleStorageStorageBackend > MerkleStorageGeneric<Backend> {

    /// Get value. Staging area is checked first, then last (checked out) commit.
    pub fn get(&mut self, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let root = &self.get_staged_root()?;
        let root_hash = self.hash_tree(&root);

        self.get_from_tree(&root_hash, key)
    }

    /// Get value. Staging area is checked first, then last (checked out) commit.
    pub fn get_by_prefix(&mut self, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let root = self.get_staged_root()?;
        self._get_key_values_by_prefix(root, prefix)
    }
    /// Get value from historical context identified by commit hash.
    pub fn get_history(&self, commit_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let commit = self.get_commit(commit_hash)?;

        self.get_from_tree(&commit.root_hash, key)
    }


    fn get_from_tree(&self, root_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let mut full_path = key.clone();
        let file = full_path.pop().ok_or(MerkleError::KeyEmpty)?;
        let path = full_path;
        let root = self.get_tree(root_hash)?;
        let node = self.find_tree(&root, &path)?;

        let node = match node.get(&file) {
            None => return Err(MerkleError::ValueNotFound { key: self.key_to_string(key) }),
            Some(entry) => entry,
        };
        match self.get_entry(&node.entry_hash)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob { key: self.key_to_string(key) })
        }
    }

    // TODO: recursion is risky (stack overflow) and inefficient, try to do it iteratively..
    fn get_key_values_from_tree_recursively(&self, path: &str, entry: &Entry, entries: &mut Vec<(ContextKey, ContextValue)> ) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob) => {
                // push key-value pair
                entries.push((self.string_to_key(path), blob.to_vec()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(key, child_node)| {
                    let fullpath = path.to_owned() + "/" + key;
                    match self.get_entry(&child_node.entry_hash) {
                        Err(_) => Ok(()),
                        Ok(entry) => self.get_key_values_from_tree_recursively(&fullpath, &entry, entries),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_key_values_from_tree_recursively(path, &entry, entries),
                }
            }
        }
    }

    pub fn get_key_values_by_prefix(&self, context_hash: &EntryHash, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let commit = self.get_commit(context_hash)?;
        let root_tree = self.get_tree(&commit.root_hash)?;
        self._get_key_values_by_prefix(root_tree, prefix)
    }

    fn _get_key_values_by_prefix(&self, root_tree: Tree, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_tree(&root_tree, prefix)?;
        let mut keyvalues: Vec<(ContextKey, ContextValue)> = Vec::new();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.get_entry(&child_node.entry_hash)?;
            let delimiter: &str;
            if prefix.is_empty() {
                delimiter = "";
            } else {
                delimiter = "/";
            }
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            self.get_key_values_from_tree_recursively(&fullpath, &entry, &mut keyvalues)?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }

    /// Flush the staging area and and move to work on a certain commit from history.
    pub fn checkout(&mut self, context_hash: &EntryHash) -> Result<(), MerkleError> {
        let commit = self.get_commit(&context_hash)?;
        self.current_stage_tree = Some(self.get_tree(&commit.root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        self.last_commit = Some(commit);
        self.staged = HashMap::new();
        self.map_stats.staged_area_elems = 0;
        Ok(())
    }

    /// Take the current changes in the staging area, create a commit and persist all changes
    /// to database under the new commit. Return last commit if there are no changes, that is
    /// empty commits are not allowed.
    pub fn commit(&mut self,
                  time: u64,
                  author: String,
                  message: String
    ) -> Result<EntryHash, MerkleError> {
        let staged_root = self.get_staged_root()?;
        let staged_root_hash = self.hash_tree(&staged_root);
        let parent_commit_hash= self.last_commit.as_ref()
            .map_or(None, |c| Some(self.hash_commit(&c)));

        let new_commit = Commit {
            root_hash: staged_root_hash, parent_commit_hash, time, author, message,
        };
        let entry = Entry::Commit(new_commit.clone());
        self.put_to_staging_area(&self.hash_commit(&new_commit), entry.clone());
        self.persist_staged_entry_to_db(&entry)?;

        self.staged = HashMap::new();
        self.map_stats.staged_area_elems = 0;
        self.last_commit = Some(new_commit.clone());
        Ok(self.hash_commit(&new_commit))
    }

    /// Set key/val to the staging area.
    pub fn set(&mut self, key: &ContextKey, value: &ContextValue) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._set(&root, key, value)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _set(&mut self, root: &Tree, key: &ContextKey, value: &ContextValue) -> Result<EntryHash, MerkleError> {
        let blob_hash = self.hash_blob(&value);
        self.put_to_staging_area(&blob_hash, Entry::Blob(value.clone()));
        let new_node = Node { entry_hash: blob_hash, node_kind: NodeKind::Leaf };
        let instant = Instant::now();
        let rv = self.compute_new_root_with_change(root, &key, Some(new_node));
        let elapsed = instant.elapsed().as_nanos() as f64;
        if self.set_exec_times >= self.set_exec_times_to_discard.into() {
            self.cumul_set_exec_time += elapsed;
        }
        self.set_exec_times += 1;
        rv
    }

    /// Delete an item from the staging area.
    pub fn delete(&mut self, key: &ContextKey) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._delete(&root, key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _delete(&mut self, root: &Tree, key: &ContextKey) -> Result<EntryHash, MerkleError> {
        if key.is_empty() { return Ok(self.hash_tree(root)); }

        self.compute_new_root_with_change(root, &key, None)
    }

    /// Copy subtree under a new path.
    /// TODO Consider copying values!
    pub fn copy(&mut self, from_key: &ContextKey, to_key: &ContextKey) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._copy(&root, from_key, to_key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _copy(&mut self, root: &Tree, from_key: &ContextKey, to_key: &ContextKey) -> Result<EntryHash, MerkleError> {
        let source_tree = self.find_tree(root, &from_key)?;
        let source_tree_hash = self.hash_tree(&source_tree);
        Ok(self.compute_new_root_with_change(
            &root, &to_key, Some(self.get_non_leaf(source_tree_hash)))?)
    }

    /// Get a new tree with `new_entry_hash` put under given `key`.
    ///
    /// # Arguments
    ///
    /// * `root` - Tree to modify
    /// * `key` - path under which the changes takes place
    /// * `new_entry_hash` - None for deletion, Some for inserting a hash under the key.
    fn compute_new_root_with_change(&mut self,
                                    root: &Tree,
                                    key: &[String],
                                    new_node: Option<Node>,
    ) -> Result<EntryHash, MerkleError> {
        if key.is_empty() {
            return Ok(new_node.unwrap_or_else(
                || self.get_non_leaf(self.hash_tree(root))).entry_hash);
        }

        let last = key.last().unwrap();
        let path = &key[..key.len() - 1];
        let mut tree = self.find_tree(root, path)?;

        match new_node {
            None => tree.remove(last),
            Some(new_node) => {
                tree.insert(last.clone(), new_node)
            }
        };

        if tree.is_empty() {
            self.compute_new_root_with_change(root, path, None)
        } else {
            let new_tree_hash = self.hash_tree(&tree);
            self.put_to_staging_area(&new_tree_hash, Entry::Tree(tree));
            self.compute_new_root_with_change(
                root, path, Some(self.get_non_leaf(new_tree_hash)))
        }
    }

    /// Find tree by path. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    fn find_tree(&self, root: &Tree, key: &[String]) -> Result<Tree, MerkleError> {
        if key.is_empty() { return Ok(root.clone()); }

        let child_node = match root.get(key.first().unwrap()) {
            Some(hash) => hash,
            None => return Ok(Tree::new()),
        };

        match self.get_entry(&child_node.entry_hash)? {
            Entry::Tree(tree) => {
                self.find_tree(&tree, &key[1..])
            }
            Entry::Blob(_) => Ok(Tree::new()),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            })
        }
    }

    /// Get latest staged tree. If it's empty, init genesis  and return genesis root.
    fn get_staged_root(&mut self) -> Result<Tree, MerkleError> {
        match &self.current_stage_tree {
            None => {
                let tree = Tree::new();
                self.put_to_staging_area(&self.hash_tree(&tree), Entry::Tree(tree.clone()));
                self.map_stats.current_tree_elems = tree.len() as u64;
                Ok(tree)
            }
            Some(tree) => {
                self.map_stats.current_tree_elems = tree.len() as u64;
                Ok(tree.clone())
            }
        }
    }

    fn put_to_staging_area(&mut self, key: &EntryHash, value: Entry) {

        self.staged.insert(*key, value);
        self.map_stats.staged_area_elems = self.staged.len() as u64;
    }

    /// Persists an entry and its descendants from staged area to database on disk.
    fn persist_staged_entry_to_db(& self, entry: &Entry) -> Result<(), MerkleError> {
        let mut transaction = self.db.create_transaction();
        // build list of entries to be persisted
        self.get_entries_recursively(entry, &mut transaction)?;
        return self.db.execute_transaction(transaction);
    }

    /// Builds vector of entries to be persisted to DB, recursively
    fn get_entries_recursively(&self, entry: &Entry, batch: &mut WriteTransaction) -> Result<(), MerkleError> {

        //passing entry by reference is tricky as its recursive function...
        batch.push((self.hash_entry(entry), entry.clone()) );

        match entry {
            Entry::Blob(_) => Ok(()),
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(_, child_node)| {
                    match self.staged.get(&child_node.entry_hash) {
                        None => Ok(()),
                        Some(entry) => self.get_entries_recursively(entry, batch),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_entries_recursively(&entry, batch),
                }
            }
        }
    }

    fn hash_entry(&self, entry: &Entry) -> EntryHash {
        match entry {
            Entry::Commit(commit) => self.hash_commit(&commit),
            Entry::Tree(tree) => self.hash_tree(&tree),
            Entry::Blob(blob) => self.hash_blob(blob),
        }
    }

    fn hash_commit(&self, commit: &Commit) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();
        hasher.update(&(HASH_LEN as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.root_hash).expect("hasher");

        if commit.parent_commit_hash.is_none() {
            hasher.update(&(0 as u64).to_be_bytes()).expect("hasher");
        } else {
            hasher.update(&(1 as u64).to_be_bytes()).expect("hasher"); // # of parents; we support only 1
            hasher.update(&(commit.parent_commit_hash.unwrap().len() as u64).to_be_bytes()).expect("hasher");
            hasher.update(&commit.parent_commit_hash.unwrap()).expect("hasher");
        }
        hasher.update(&(commit.time as u64).to_be_bytes()).expect("hasher");
        hasher.update(&(commit.author.len() as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.author.clone().into_bytes()).expect("hasher");
        hasher.update(&(commit.message.len() as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.message.clone().into_bytes()).expect("hasher");

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn hash_tree(&self, tree: &Tree) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();

        hasher.update(&(tree.len() as u64).to_be_bytes()).expect("hasher");
        tree.iter().for_each(|(k, v)| {
            hasher.update(&self.encode_irmin_node_kind(&v.node_kind)).expect("hasher");
            hasher.update(&[k.len() as u8]).expect("hasher");
            hasher.update(&k.clone().into_bytes()).expect("hasher");
            hasher.update(&(HASH_LEN as u64).to_be_bytes()).expect("hasher");
            hasher.update(&v.entry_hash).expect("hasher");
        });

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn hash_blob(&self, blob: &ContextValue) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();
        hasher.update(&(blob.len() as u64).to_be_bytes()).expect("Failed to update hasher state");
        hasher.update(blob).expect("Failed to update hasher state");

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn encode_irmin_node_kind(&self, kind: &NodeKind) -> Vec<u8> {
        match kind {
            NodeKind::NonLeaf => vec![0, 0, 0, 0, 0, 0, 0, 0],
            NodeKind::Leaf => vec![255, 0, 0, 0, 0, 0, 0, 0],
        }
    }


    fn get_tree(&self, hash: &EntryHash) -> Result<Tree, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Tree(tree) => Ok(tree),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn get_commit(&self, hash: &EntryHash) -> Result<Commit, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Commit(commit) => Ok(commit),
            Entry::Tree(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
        }
    }

    fn get_entry(&self, hash: &EntryHash) -> Result<Entry, MerkleError> {
        match self.staged.get(hash) {
            None => {
                match self.db.get_value(hash) {
                    None => Err(MerkleError::EntryNotFound { hash: HashType::ContextHash.bytes_to_string(hash) } ),
                    Some(entry) => Ok(entry),
                }
            }
            Some(entry) => Ok(entry.clone()),
        }
    }

    fn get_non_leaf(&self, hash: EntryHash) -> Node {
        Node { node_kind: NodeKind::NonLeaf, entry_hash: hash }
    }

    fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    fn string_to_key(&self, string: &str) -> ContextKey {
        string.split('/').map(str::to_string).collect()
    }

    pub fn get_last_commit_hash(&self) -> Option<EntryHash> {
        match &self.last_commit {
            Some(c) => Some(self.hash_commit(&c)),
            None    => None
        }
    }



}

// small trick for backward compatibility
pub type MerkleStorage = MerkleStorageGeneric<Arc<MerkleStorageKV>>;
pub type MerkleStorageInMemory = MerkleStorageGeneric<InMemoryBackend>;

//backward compatibility
impl MerkleStorage {
    pub fn new(db: Arc<MerkleStorageKV>) -> Self {
        MerkleStorage {
            db : db,
            staged: HashMap::new(),
            current_stage_tree: None,
            last_commit: None,
            map_stats: MerkleMapStats{staged_area_elems: 0 , current_tree_elems: 0},
            cumul_set_exec_time: 0.0,
            set_exec_times: 0,
            set_exec_times_to_discard: 20,
        }
    }

    pub fn get_merkle_stats(&self) -> Result<MerkleStorageStats, MerkleError> {
        let db_stats = self.db.get_mem_use_stats()?;
        let mut avg_set_exec_time_ns: f64 = 0.0;
        if self.set_exec_times > self.set_exec_times_to_discard {
            avg_set_exec_time_ns = self.cumul_set_exec_time / ((self.set_exec_times - self.set_exec_times_to_discard) as f64);
        }
        let perf = MerklePerfStats { avg_set_exec_time_ns: avg_set_exec_time_ns};
        Ok(MerkleStorageStats{ rocksdb_stats: db_stats, map_stats: self.map_stats, perf_stats: perf })
    }
}

// in memory implementation
impl MerkleStorageInMemory {
    pub fn new() -> Self {
        MerkleStorageInMemory {
            db : InMemoryBackend::new(InMemoryBackend::get_db_path()),
            staged: HashMap::new(),
            current_stage_tree: None,
            last_commit: None,
            map_stats: MerkleMapStats{staged_area_elems: 0 , current_tree_elems: 0},
            cumul_set_exec_time: 0.0,
            set_exec_times: 0,
            set_exec_times_to_discard: 20,
        }
    }
}


pub mod utils{
    use super::*;
    use std::path::Path;
    use std::fs;
    use rocksdb::{DB, Options};


    fn open_db<P: AsRef<Path>>(path: P, cache: &Cache) -> DB {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);

    DB::open_cf_descriptors(&db_opts, path, vec![MerkleStorageSchema::descriptor(&cache)]).unwrap()
    }

    pub fn get_db_name() -> &'static str { "_merkle_db_test" }

    fn get_db(cache: &Cache) -> DB { open_db(get_db_name(), &cache) }

    pub fn get_storage_deprecated(cache: &Cache) -> MerkleStorage { MerkleStorage::new(Arc::new(get_db(&cache))) }

    // keep the same signature ...
    pub fn get_storage() -> MerkleStorageInMemory { MerkleStorageInMemory::new() }

    pub fn clean_db() {
        let _ = DB::destroy(&Options::default(), get_db_name());

        println!("removing database '{}'", InMemoryBackend::get_db_path());
        let _ = fs::remove_dir_all(InMemoryBackend::get_db_path());
    }
}


#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use super::*;
    use serial_test::serial;
    use rocksdb::{DB, Options};
    use utils::{get_storage,get_storage_deprecated, get_db_name,clean_db};
    /*
    * Tests need to run sequentially, otherwise they will try to open RocksDB at the same time.
    */
    #[test]
    #[serial]
    fn test_tree_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string(), "foo".to_string()], &vec![97, 98, 99]); // abc
        storage.set(&vec!["b".to_string(), "boo".to_string()], &vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], &vec![97, 98, 99, 100]);
        storage.set(&vec!["x".to_string()], &vec![97]);
        storage.set(&vec!["one".to_string(), "two".to_string(), "three".to_string()], &vec![97]);
        let tree = storage.current_stage_tree.clone().unwrap().clone();

        let hash = storage.hash_tree(&tree);

        assert_eq!([0xDB, 0xAE, 0xD7, 0xB6], hash[0..4]);
    }

    #[test]
    #[serial]
    fn test_commit_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string()], &vec![97, 98, 99]);

        let commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        assert_eq!([0xCF, 0x95, 0x18, 0x33], commit.unwrap()[0..4]);

        storage.set(&vec!["data".to_string(), "x".to_string()], &vec![97]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());

        assert_eq!([0xCA, 0x7B, 0xC7, 0x02], commit.unwrap()[0..4]);
        // full irmin hash: ca7bc7022ffbd35acc97f7defb00c486bb7f4d19a2d62790d5949775eb74f3c8
    }

    #[test]
    #[serial]
    fn test_multiple_commit_hash() {
        let mut storage = get_storage();
        let _commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string()], &vec![97]);
        storage.copy(&vec!["data".to_string(), "a".to_string()], &vec!["data".to_string(), "b".to_string()]);
        storage.delete(&vec!["data".to_string(), "b".to_string(), "x".to_string()]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());

        assert_eq!([0x9B, 0xB0, 0x0D, 0x6E], commit.unwrap()[0..4]);
    }
    


    #[test]
    #[serial]
    fn get_test() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];

        {
            let mut storage = get_storage();
            storage.set(key_abc, &vec![1u8, 2u8]);
            storage.set(key_abx, &vec![3u8]);
            assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
            assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_az, &vec![4u8]);
            storage.set(key_abx, &vec![5u8]);
            storage.set(key_d, &vec![6u8]);
            storage.set(key_eab, &vec![7u8]);
            assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        let storage = get_storage();
        assert_eq!(storage.get_history(&commit1, key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);
    }

    #[test]
    #[serial]
    fn test_copy() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![1 as u8]);
        storage.copy(&vec!["a".to_string()], &vec!["z".to_string()]);

        assert_eq!(
            vec![1 as u8],
            storage.get(&vec!["z".to_string(), "b".to_string(), "c".to_string()]).unwrap());
        // TODO test copy over commits
    }

    #[test]
    #[serial]
    fn test_delete() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2 as u8]);
        storage.set(key_abx, &vec![3 as u8]);
        storage.delete(key_abx);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit1, &key_abx).is_err());
    }

    #[test]
    #[serial]
    fn test_deleted_entry_available() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![2 as u8]);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        storage.delete(key_abc);
        let _commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert_eq!(vec![2 as u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    #[test]
    #[serial]
    fn test_delete_in_separate_commit() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2 as u8]).unwrap();
        storage.set(key_abx, &vec![3 as u8]).unwrap();
        storage.commit(0, "".to_string(), "".to_string()).unwrap();

        storage.delete(key_abx);
        let commit2 = storage.commit(
            0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit2, &key_abx).is_err());
    }

    #[test]
    #[serial]
    fn test_checkout() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];

        {
            let mut storage = get_storage();
            storage.set(key_abc, &vec![1u8]).unwrap();
            storage.set(key_abx, &vec![2u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_abc, &vec![3u8]).unwrap();
            storage.set(key_abx, &vec![4u8]).unwrap();
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        let mut storage = get_storage();
        storage.checkout(&commit1);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
        // this set be wiped by checkout
        storage.set(key_abc, &vec![8u8]).unwrap();

        storage.checkout(&commit2);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
    }

    #[test]
    #[serial]
    fn test_persistence_over_reopens() {
        { clean_db(); }

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let commit1;
        {
            let mut storage = get_storage();
            let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
            storage.set(key_abc, &vec![2 as u8]).unwrap();
            storage.set(key_abx, &vec![3 as u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        let storage = get_storage();
        assert_eq!(vec![2 as u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    #[test]
    #[serial]
    fn test_get_errors() {
        { clean_db(); }

        let mut storage = get_storage();

        let res = storage.get(&vec![]);
        assert!(if let MerkleError::KeyEmpty = res.err().unwrap() { true } else { false });

        let res = storage.get(&vec!["a".to_string()]);
        assert!(if let MerkleError::ValueNotFound { .. } = res.err().unwrap() { true } else { false });
    }


    #[test]
    #[serial]
    fn test_db_error() { // Test a DB error by writing into a read-only database.
        {
            clean_db();
            let cache = Cache::new_lru_cache(32 * 1024 * 1024).unwrap();
            get_storage_deprecated(&cache);
        }

        let db = DB::open_for_read_only(
            &Options::default(), get_db_name(), true).unwrap();
        let mut storage = MerkleStorage::new(Arc::new(db));
        storage.set(&vec!["a".to_string()], &vec![1u8]);
        let res = storage.commit(
            0, "".to_string(), "".to_string());

        assert!(if let MerkleError::DBError { .. } = res.err().unwrap() { true } else { false });
    }

    #[test]
    #[serial]
    fn test_both_storages_returns_the_same_hashes() {
        let cache = Cache::new_lru_cache(32 * 1024 * 1024).unwrap();
        let mut rocket_storage = get_storage_deprecated(&cache);
        let mut in_memory_storage = get_storage();

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];

        rocket_storage.set(key_abc,&vec![2 as u8] ).unwrap();
        in_memory_storage.set(key_abc,&vec![2 as u8] ).unwrap();
        let rocket_hash = rocket_storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        let in_mem_hash = in_memory_storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        assert_eq!(rocket_hash, in_mem_hash);
    }


    #[test]
    #[serial]
    fn test_persitent_storage() {
        use std::fs;
        let _ = fs::remove_dir_all("/tmp/test");
        let hash2: EntryHash = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut root = Tree::new();
        let hash1: EntryHash = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32];
        let node = Node { node_kind: NodeKind::Leaf, entry_hash: hash1 };
        root.insert(String::from("hello"), node.clone());
        let entry: Entry = Entry::Tree(root.clone());
        let payload: Vec<u8> = bincode::serialize(&entry).unwrap();

        {
            let storage = sled::Config::default().path(&"/tmp/test").open().unwrap();
            // let config = sled::Config::default().path("asdfasdf").open().unwrap();
            storage.insert(hash2, payload).unwrap();
        }

        {
            let storage = sled::Config::default().path(&"/tmp/test").open().unwrap();
            let x = storage.get(hash2).unwrap().unwrap();
            let result: Entry = bincode::deserialize(&x).unwrap();


            assert_eq!(node.clone(), node.clone());
            assert_eq!(root.clone(), root.clone());
            assert_eq!(entry.clone(), entry.clone());
            assert_eq!(result, entry.clone());
            // assert!()
        }
    }


    fn gen_unique_payloads(n: u32, length: usize) -> Vec<Vec<u8>> {
        let payloads: Vec<Vec<u8>> = (0..n).map(|i| {
            let mut payload = bincode::serialize(&(i as u32)).unwrap();
            payload.resize(length, 0);
            return payload;
        }).collect();
        return payloads;
    }



}


#[allow(unused_import)]
mod benchmarking {

    extern crate test;
    use test::Bencher;
    use super::*;

    use itertools::iproduct;
    use utils::{clean_db, get_storage_deprecated, get_storage};

    fn gen_unique_payloads(n : u32, length: usize) -> Vec<Vec<u8>> {
        let payloads: Vec<Vec<u8>> = (0..n).map(|i| {
            let mut payload = bincode::serialize(&(i as u32)).unwrap();
            payload.resize(length, 0);
            return payload;
        }).collect();
        return payloads;
    }


    fn gen_unique_paths(count: u32) -> Vec<Vec<String>> {
        let mut paths : Vec<Vec<String>> = vec![];
        for i in 1..count {
            paths.push((i as u32).to_be_bytes().iter().map(|b| b.to_string()).collect());
        }
        return paths;
    }

    #[bench]
    pub fn bench_in_memory_1000_elements_binary_tree(b: &mut Bencher) {
        clean_db();
        let payloads = gen_unique_payloads(1000, 1000); 
        let paths = gen_unique_paths(payloads.len() as u32);

        let mut storage = get_storage();
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
            }
            storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        });
    }

    #[bench]
    pub fn bench_in_memory_1000_elements_binary_tree_many_commits(b: &mut Bencher) {
        clean_db();
        let payloads = gen_unique_payloads(1000, 1000); 
        let paths = gen_unique_paths(payloads.len() as u32);

        let mut storage = get_storage();
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
                storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
            }
        });
    }

    #[bench]
    pub fn bench_in_memory_1000_elements_dense_tree(b: &mut Bencher) {
        clean_db();
        let mut paths : Vec<Vec<String>> = vec![];
        for (x1,x2,x3) in iproduct!(0..10, 0..10, 0..10) {
            paths.push(vec![x1,x2,x3].iter().map(|x| x.to_string()).collect());
        }
        let payloads = gen_unique_payloads(paths.len() as u32, paths.len());

        let mut storage = get_storage();
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
            }
            storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        });
    }

    #[bench]
    pub fn bench_in_memory_1000_elements_dense_tree_many_commits(b: &mut Bencher) {
        clean_db();
        let mut paths : Vec<Vec<String>> = vec![];
        for (x1,x2,x3) in iproduct!(0..10, 0..10, 0..10) {
            paths.push(vec![x1,x2,x3].iter().map(|x| x.to_string()).collect());
        }
        let payloads = gen_unique_payloads(paths.len() as u32, paths.len());

        let mut storage = get_storage();
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
                storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
            }
        });
    }

    #[bench]
    pub fn bench_rocket_1000_elements_binary_tree(b: &mut Bencher) {
        clean_db();
        let payloads = gen_unique_payloads(1000, 1000); 
        let paths = gen_unique_paths(payloads.len() as u32);

        let cache = Cache::new_lru_cache(32 * 1024 ).unwrap();
        let mut storage = get_storage_deprecated(&cache);

        // MerkleStorage::new
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
            }
            storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        });
    }

    #[bench]
    pub fn bench_rocket_1000_elements_binary_tree_many_commits(b: &mut Bencher) {
        clean_db();
        let payloads = gen_unique_payloads(1000, 1000); 
        let paths = gen_unique_paths(payloads.len() as u32);

        let cache = Cache::new_lru_cache(32 * 1024 ).unwrap();
        let mut storage = get_storage_deprecated(&cache);

        // MerkleStorage::new
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
                storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
            }
        });
    }

    #[bench]
    pub fn bench_rocket_1000_elements_dense_tree(b: &mut Bencher) {
        clean_db();
        let mut paths : Vec<Vec<String>> = vec![];
        for (x1,x2,x3) in iproduct!(0..10, 0..10, 0..10) {
            paths.push(vec![x1,x2,x3].iter().map(|x| x.to_string()).collect());
        }
        let payloads = gen_unique_payloads(paths.len() as u32, paths.len());

        let cache = Cache::new_lru_cache(32 * 1024  ).unwrap();
        let mut storage = get_storage_deprecated(&cache);

        // MerkleStorage::new
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
            }
            storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
        });
    }

    #[bench]
    pub fn bench_rocket_1000_elements_dense_tree_many_commits(b: &mut Bencher) {
        clean_db();
        let mut paths : Vec<Vec<String>> = vec![];
        for (x1,x2,x3) in iproduct!(0..10, 0..10, 0..10) {
            paths.push(vec![x1,x2,x3].iter().map(|x| x.to_string()).collect());
        }
        let payloads = gen_unique_payloads(paths.len() as u32, paths.len());

        let cache = Cache::new_lru_cache(32 * 1024  ).unwrap();
        let mut storage = get_storage_deprecated(&cache);

        // MerkleStorage::new
        let data: Vec<(&Vec<String>,Vec<u8>)> = paths.iter().zip(payloads).collect();
        b.iter(|| {
            for (key,val) in &data{
                storage.set(*key,&val).unwrap();
                storage.commit(0,String::from("anonymouse"), String::from("commit")).unwrap();
            }
        });
    }
}
