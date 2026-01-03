# Perfect Bloom Filter
Probabilistic accuracy guaranteed

What is this?
- In-memory hybrid dynamically scalable cascading multidimensional bloom filter with a simple API.

Notes:
- Uses file IO/disk space for bitvec atomic hot swap.
- Rehash feature & filter specs tunable within set parameters.

How to use: 
- The input to the filter is bytes, the binary representation doesn't matter, however, it must be kept to a consistent representation per data type in your application to avoid undefined behavior. 

Max key size: 1MB.
- If a key surpasses 1MB the reader will assume file is corrupt and fail the shard rehash operation to avoid corrupting in memory representation (which increases risk of false positives of not atomically handled in your application).

Concurrency:
- Due to the granular internal locking mechanisms the filter is safe to use in both sync and async environments without any additional locking required by the user. The current specs are configurable to a maximum concurrency factor of 4096 write operation and non blocking reads.


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
    .with_initial_capacity(Capacity::Medium);


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


