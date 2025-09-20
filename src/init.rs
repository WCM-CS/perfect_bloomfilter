use anyhow::{Result, anyhow};
use bitvec::{bitvec, vec::BitVec};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet}, fs, io::{self, BufRead, Cursor, Write}, path::Path, sync::{Arc, Mutex, RwLock}, thread::{self}, time::Duration
};
use threadpool::ThreadPool;
use csv::Writer;


//pub const HASH_SEED_SELECTION: [u32; 6] = [9, 223, 372, 530, 775, 954];
pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x9747b28c,
    0x239b961b,
    0xabcdef12,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const STATIC_VECTOR_LENGTH_OUTER: u32 = 4096;
static OUTER_BLOOM_STARTING_MULT: u32 = 13;

static OUTER_BLOOM_DEFAULT_LENGTH: u64 = 2_u64.pow( OUTER_BLOOM_STARTING_MULT);



struct PerfectBloomFilter {
    outer_filter: Arc<OuterBlooms>,
    inner_filter: Arc<InnerBlooms>,
}

impl PerfectBloomFilter {
    pub fn new() -> Result<Self> {
        let outer_filter = Arc::new(OuterBlooms::default());
        let inner_filter = Arc::new(InnerBlooms::default());
    Ok(Self {
            outer_filter,
            inner_filter,
        })
    }


    pub fn contains_insert(&mut self, key: &str) -> Result<bool> {
        let outer_res = self.outer_filter.contains_and_insert(key)?;
        let inner_res = self.inner_filter.contains_and_insert(key)?;

        Ok(outer_res && inner_res)
    }
}

pub trait BloomFilter {
    type CollisionResult;
    //fn lookup(&self, key: &str) -> Result<Bool>;
    //fn insert(&self, key: &str) -> Result<()>;
    fn bloom_hash(&self, vector_partitions: &[u32], key: &str) -> Result<HashMap<u32, Vec<u64>>>;
    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> Result<CollisionResult>;
    fn bloom_insert(&self, outer_hashed: &HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str);
    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>>;
}   



pub fn raise_to_the_power_of_x(x: u32) -> u64{
    2_u64.pow(x)
}



// m = bloom filter len, b = bits per key threshold
//k = number of hashes, n = keys 

pub enum FilterType {
    Outer,
    Inner,
}

