# Perfect Bloom Filter
Probabilistic accuracy guaranteed

Dynamically scalable cascading multidimensional bloom filter 

<details>
  <summary>Summary</summary>

  In memory hybrid filter system. Concept design inspired by perfect hashing. Uses binary hashing with murmur3 for both the double hashing into the bloom filters and jump consistent hashing for determining vector partitions. Requires storage to utilize bloom filter rehashing feature.


  System as is:    
  3 level cascade.   
     
  Umbrella outer bloom 2^32 7 hashes
  Vector 4096 buckets, each key slots into two buckets/inner bloomfilters 
  Inner blooms 2^20 13 hashes
  Both inner blooms must collide for it to be a false positive

  Current config set to: 1 in 2.4 trillion queries



  Refactoring to use a more modular and scalable design

  No big bloom only multidimensional blooms using array intead of a vector for reduces overhead during the hash slot sharding. Also refactoring to use arenas for each bloom filter and arc swap to make it async compatible and allow for reduced overhead for key insertions via the bloom being in a preallocated contiguous arena in the healp and accessed via pointer and dropped in constant time. This paired with the existing jump hash partition for hash slot sharding and double hash for bloom filter insertion should work well. 

  Also thinking about wasy of minimizing overhead for io suck as usisng a protobuf instead of embedded db or simple txt file. 


</details>