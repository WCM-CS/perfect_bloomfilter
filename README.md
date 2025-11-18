# Perfect Bloom Filter
Probabilistic accuracy guaranteed

What is this?
In memory hybrid dynamically scalable cascading multidimensional bloom filter 

Notes:
- Uses file IO/disk space.
- Rehash feature configurable by passing in the config.

Sync:
    Due to the internal locking mechanisms sync is safe to use in both sync and async environments. Sync is the faster option for almost all operations. However sync can be limiting in highly concurrent green threaded environments. Sync writers block os threads and paralellism is limited by the available os threads.

Async:
    Async option is slower than sync but supports the full logical concurrency of 2^12 writers per shard-vec and it's writers are non blocking. Granted this is acheived via tokio runtime thus the green threads compute are still bounded by available os threads. This runs with maximum concurrency but the same parallelism as the sync variation minus the writers blocking. 
