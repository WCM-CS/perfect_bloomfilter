# Perfect Bloom Filter
Probabilistic accuracy guaranteed

What is this?
- In-memory hybrid dynamically scalable cascading multidimensional bloom filter 

Notes:
- Uses file IO/disk space for bitvec atomic hot swap.
- Rehash feature & filter specs configurable via new_with_config.

How to use: 
- Input to the filter is bytes, the binary representation doesnt matter, however, it must be kept to a consistent representation per data type in your application to avoid undefined behavior. 

Max key size: 1MB.
- If a key surpasses 1MB the readder will assume file is corrupt and fail a shard rehash operation to avoid corrupting in memory representation (which increases risk of false positives of not atomically handled in your application).

Concurrency:
- Due to the granular internal locking mechanisms the filter is safe to use in both sync and async environments without any additional locking required by the user. The current specs are configurable to a maximum concurrency factor of 4096 write operation and non blocking reads.


```markdown
```rust

// Bloom filter imports
use perfect_bloomfilter::config::*;
use perfect_bloomfilter::filter::PerfectBloomFilter;


// Build a config (Optional)
let default_config = BloomFilterConfig::new()
    .with_rehash(true)
    .with_throughput(Throughput::Medium)
    .with_accuracy(Accuracy::Medium)
    .with_initial_capacity(Capacity::Medium);


// Instantiate Perfect Bloomfilter
let pf = PerfectBloomFilter::new_with_config(config);

// Or do this if you're okay with defaults: let pf = PerfectBloomFilter::new();





