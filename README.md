# Perfect Bloom Filter
Probabilistic accuracy guaranteed

What is this?
- In-memory hybrid dynamically scalable cascading multidimensional bloom filter with a simple API. Rehashes and hotswaps filter shards when probability of a false positive hits the threshold. Crossbeam channel is used for offloading file IO. Shards use parking lot RwLocks but inserts and contains oprtations do not lock, they atomically mutate shards internals. Only background workers lock shards during part of their operation. Currently configured to use a sandboxed mimalloc arena allocator.

Notes:
- Uses file IO/disk space for bitvec atomic hot swap.
- Rehash feature & filter specs tunable within set parameters.

How to use: 
- The input to the filter is bytes, the binary representation doesn't matter be, le, ne. However, it must be kept to a consistent representation per data type in your application to avoid undefined behavior. 

Max key size: 1MB.
- If a key surpasses 1MB the reader will assume file is corrupt and fail the shard rehash operation to avoid corrupting in memory representation (which increases risk of false positives of not atomically handled in your application).

Concurrency:
- Due to the granular internal locking mechanisms the filter is safe to use in both sync and async environments without any additional locking required by the user. The current specs are configurable to a maximum concurrency factor of 4096 write operation and non locking reads and writes.


Perfect Bloomfilter Optimization Summary

Stage,Strategy,Rate (Keys/s),Boost,Bottleneck Removed
Base,Blocking Mutex + Sync IO,"33,000",1.0x,Baseline
Round A,Non-blocking IO Channel,"50,000",1.5x,Disk/File I/O Latency
Round B,Atomic Inserts (Lock-free),"95,000",2.9x,Thread Contention & Context Switching
Round C,Sandboxed Arena Allocator,"420,000",12.7x,Kernel Syscalls & Heap Fragmentation


Test command example
(ulimit -v 2097152; MIMALLOC_RESERVE_OS_MEMORY="2g" MIMALLOC_DISALLOW_OS_ALLOC="1" MIMALLOC_ARENA_RESERVE_HUGE="1" MIMALLOC_SHOW_STATS="1" cargo test --release -- --nocapture)

Prod command example
MIMALLOC_RESERVE_OS_MEMORY="2g" MIMALLOC_DISALLOW_OS_ALLOC="1" MIMALLOC_ARENA_RESERVE_HUGE="1" cargo build --release




```markdown
```rust

// Bloom filter imports
use perfect_bloomfilter::config::*; // (Optional)
use perfect_bloomfilter::filter::PerfectBloomFilter;


// Build a config (Optional)
let default_config = BloomFilterConfig::new()
    .with_rehash(true)
    .with_throughput(Throughput::Medium)
    .with_accuracy(Accuracy::Medium)
    .with_initial_capaci(Capacity::Medium)
    .with_worker_cores(Workers::Cores1);

let max_config = BloomFilterConfig::new()
    .with_rehash(true)
    .with_throughput(Throughput::High)
    .with_accuracy(Accuracy::High)
    .with_initial_capacity(Capacity::VeryHigh)
    .with_worker_cores(Workers::Cores8);

let minimal_filter_no_rehash = BloomFilterConfig::new()
    .with_rehash(false) // No dynamic scaling
    .with_throughput(Throughput::Low)
    .with_accuracy(Accuracy::Medium)
    .with_initial_capacity(Capacity::Low)
    .with_worker_cores(Workers::Cores1);


// Instantiate Perfect Bloomfilter
let pf = PerfectBloomFilter::new_with_config(default_config);
// Or do this if you're okay with the defaults: let pf = PerfectBloomFilter::new();


// Key examples 
let key_str_bytes = "gamma".as_bytes();   // casting a str to bytes
let key_int_bytes = &5_u32.to_be_bytes(); // casting a u32 to bytes


// Perfect Bloomfilter: insert(&self, key: &[u8]) -> anyhow::Result<()>
let _ = pf.insert(key_str_bytes);
let _ = pf.insert(key_int_bytes);


// Perfect Bloomfilter: contains(&self, key: &[u8]) -> bool
assert!(pf.contains(key_str_bytes));
assert!(pf.contains(key_int_bytes));
assert!(!pf.contains("delta".as_bytes()));
