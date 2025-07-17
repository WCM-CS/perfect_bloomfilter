# perfect_bloomfilter
Probabilistic accuracy guaranteed

Data system for fast, memory efficient and scalable existence checks.

Summary
    Hybrid in memory filter, requires storage for dynamic scalability. Uses a multi-dimentional bloomfilter structure inspired by perfect hashing. Allows granular rehashing of inner bloomfilters in order to stay below a given false positive rate. Automatic internal tracking and scaling via periodic metadata computations. Multiple vector partition slots ensure we detect partial collisions before complete false positives occur. Writes keys to disk under vector partition key for rehashing and scaling inner blooms. Outer bloom is currently static and is recommended to be oversized to fit your needs given your key upper bounds and desired false positive rate.