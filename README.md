# Description

Your task is to reverse-engineer/debug our current implementation of merke_tree
storage and replace key-value store (rocks-db) with in memory Rust based data structure and one persistent key-value store

Please develop your solution focusing on the following aspects:
> - The solution must be available as a Git repository which can be accessed by the solution reviewers. Please fork https://github.com/simplestaking/tezedge

I believe It is as you are reading this doc [https://github.com/mateuszaaa/tezedge](https://github.com/mateuszaaa/tezedge)

> - The solution reviewers must be able to build the project for testing and benchmarking.

~~~
$ cd tezedge/storage
$ cargo build
$ cargo test --color=always --package storage --lib merkle_storage::tests
  
running 14 tests
test merkle_storage::tests::get_test ... ok
test merkle_storage::tests::test_commit_hash ... ok
test merkle_storage::tests::test_copy ... ok
test merkle_storage::tests::test_db_error ... ok
test merkle_storage::tests::test_delete ... ok
test merkle_storage::tests::test_delete_in_separate_commit ... ok
test merkle_storage::tests::test_get_errors ... ok
test merkle_storage::tests::test_deleted_entry_available ... ok
test merkle_storage::tests::test_multiple_commit_hash ... ok
test merkle_storage::tests::test_persistence_over_reopens ... ok
test merkle_storage::tests::test_persitent_storage ... ok
test merkle_storage::tests::test_tree_hash ... ok
test merkle_storage::tests::test_checkout ... ok
test merkle_storage::tests::test_both_storages_returns_the_same_hashes ... ok

test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 33 filtered out
~~~

> - The solution must pass all existing tests https://github.com/simplestaking/tezedge/blob/7d59ea35ec07e86c34f4e2782d8d89f73aaa57aa/storage/src/merkle_storage.rs

as above

> The solution must contain at least one extra unit test.

I added one used for checking compatibility of rocket_db and in_memory implementations. Also benchmarks may be consider as some sort of tests.

> - The solution must contain at least one performance benchmark.

I added few to compare 'old' and 'new' in memory implementation. I modeled paths for two scenarios:

- sparse 'binary' tree where each non-leaf node has 2 childs:
    -  with 'commit' call after each 'set'
    -  with single 'commit' call after all 'set' calls
- dense tree where each non-leaf node has 10 childs
    -  with 'commit' call after each 'set'
    -  with single 'commit' call after all 'set' calls

~~~
$ cd tezedge/storage
$ cargo bench
test merkle_storage::benchmarking::bench_in_memory_1000_elements_binary_tree              ... bench:  60,000,591 ns/iter (+/- 2,033,952)
test merkle_storage::benchmarking::bench_in_memory_1000_elements_binary_tree_many_commits ... bench: 139,680,808 ns/iter (+/- 5,969,031)
test merkle_storage::benchmarking::bench_in_memory_1000_elements_dense_tree               ... bench:  16,931,198 ns/iter (+/- 748,995)
test merkle_storage::benchmarking::bench_in_memory_1000_elements_dense_tree_many_commits  ... bench:  41,569,112 ns/iter (+/- 1,830,468)
test merkle_storage::benchmarking::bench_rocket_1000_elements_binary_tree                 ... bench:  66,949,413 ns/iter (+/- 1,746,483)
test merkle_storage::benchmarking::bench_rocket_1000_elements_binary_tree_many_commits    ... bench: 342,213,445 ns/iter (+/- 16,693,535)
test merkle_storage::benchmarking::bench_rocket_1000_elements_dense_tree                  ... bench:  23,945,279 ns/iter (+/- 1,267,700)
test merkle_storage::benchmarking::bench_rocket_1000_elements_dense_tree_many_commits     ... bench:  69,181,757 ns/iter (+/- 3,534,702)
~~~

- The solution must replace key-value store with in memory Rust based data structure ( for example BTreeMap ) and one persistent key-value store ( for example Sled )

In my opinion BTreeMap is more efficient in that scenario as we dont care that much about ordering. Eventually i used Sled but it seems to be an overkill
in that particular scenario. Storing mappings in a simple file instead would be enough in my opinion.

In my approach i came up with generic `MerkleStorageGeneric` that has all the common methods for both 'old' and 'new' impl

~~~rust
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
~~~

My intention was to not implement `KeyValueStoreWithSchema` for in memory implementation. The main problem with that one
is fact that it comes from third party library and is used in many places in the code so function signatures cannot be
modified easily. What is more `set`, `get` operates on serialized (bytes) data and in my solution i wanted to avoid
serialization/deserialization cost(and i believe did that),
  
i also created `MerkleStorageStorageBackend` that has solution specific implementation

~~~rust
pub trait MerkleStorageStorageBackend {

    fn create_transaction(&self) -> WriteTransaction;

    fn execute_transaction(& self, t: WriteTransaction) -> Result<(),MerkleError>;

    fn get_value(&self,key: &EntryHash) -> Option<Entry>;
}
~~~
   
> - Both the solution itself and its code must be reasonably structured.

I hope it is. I tried to use self descriptive names for both functions and variables. I also tried to implement changes in
such way that diff against origin version looks readable - to help you guys tracking differences.

> - A short README file in the root of the project must explain how to build, test and benchmark solutions. Please replace the current README file with your explanation.
done

# Notes

- In memory implemenatation is faster (that one was kind of obvious from the begining)
- Sled is an overkill for storing data - simple file would work as well
- i kind of failed to implement `WriteTransaction` as  `Vec<(EntryHash, &Entry)>`. That was mainly because of recursive function. I'm preety sure that it can be improved ...
- i didn't test concurrent access to file storage (as in origin rocket\_db implementation) - that should be added for production level code

