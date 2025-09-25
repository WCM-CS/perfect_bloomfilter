use std::sync::{Arc, RwLock, RwLockWriteGuard};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::{Lazy, OnceCell};


use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, ARRAY_SHARDS, BLOOM_HASH_FAMILY_SIZE, BLOOM_STARTING_LENGTH, BLOOM_STARTING_MULT}, utils::{concurrecy_init, CollisionResult, FilterType}};

pub static GLOBAL_PBF: OnceCell<PerfectBloomFilter> = OnceCell::new();


pub struct PerfectBloomFilter {
    pub(crate) outer_filter: ShardVector,
    pub(crate) inner_filter: ShardVector,
}

impl PerfectBloomFilter {
    pub fn new() -> &'static Self {
        GLOBAL_PBF.get_or_init(|| PerfectBloomFilter {
            outer_filter: ShardVector::default(),
            inner_filter: ShardVector::default(),
        })
    }

    pub fn contains(&self, key: &str) -> Result<bool> {
        let outer = ShardVector::existence_check(key, &FilterType::Outer)?;
        let inner = ShardVector::existence_check(key, &FilterType::Inner)?;

       Ok(outer & inner)
    }

    pub fn insert(&self, key: &str) -> Result<()> {
        ShardVector::insert_key(key, &FilterType::Outer)?;
        ShardVector::insert_key(key, &FilterType::Inner)?;

        Ok(())
    }
}


pub struct ShardVector {
    pub shard_vector: Arc<Vec<RwLock<ShardData>>>,
}

impl Default for ShardVector {
    fn default() -> Self {
        Self::new(ARRAY_SHARDS)
    }
}

impl ShardVector {
    fn new(shard_vector_length: u32) -> Self {
        let shard_vector = (0..shard_vector_length)
            .map(|_| RwLock::new(ShardData::default()))
            .collect::<Vec<_>>();

        let shard_arc = Arc::new(shard_vector);

        ShardVector { shard_vector: shard_arc }
    }

    fn existence_check(key: &str, filter_type: &FilterType) -> Result<bool>{
        let shards = array_sharding_hash(key, filter_type)?;
        let shard_vec = match filter_type {
            FilterType::Outer => &GLOBAL_PBF.get().unwrap().outer_filter.shard_vector,
            FilterType::Inner => &GLOBAL_PBF.get().unwrap().inner_filter.shard_vector,
        };

        let locked_shards = lock_shards(&shards, shard_vec);
        let shards_hashes = bloom_hash(&shards, key, filter_type, &locked_shards)?;
        let collision_results = bloom_check(&shards_hashes, filter_type, &locked_shards)?;

        let exists = match collision_results {
            CollisionResult::Zero => false,
            CollisionResult::Partial(_) => false,
            CollisionResult::Complete(_, _) => true,
            CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };
        
        Ok(exists)
    }

    fn insert_key<'a>(key: &str, filter_type: &FilterType) -> Result<()>{
        let shards = array_sharding_hash(key, filter_type)?;    
        let shard_vec = match filter_type {
            FilterType::Outer => &GLOBAL_PBF.get().unwrap().outer_filter.shard_vector,
            FilterType::Inner => &GLOBAL_PBF.get().unwrap().inner_filter.shard_vector,
        };

        let mut locked_shards = lock_shards(&shards, shard_vec);

        let shards_hashes = bloom_hash(&shards, key, filter_type, &locked_shards)?;
        bloom_insert(&shards_hashes, filter_type, &mut locked_shards)?;

        Ok(())
    }
}

fn lock_shards<'a>(
    shards: &[u32], 
    shard_vec: &'a [RwLock<ShardData>]
) -> Vec<RwLockWriteGuard<'a, ShardData>> {
    shards.iter()
        .map(|&shard_idx| shard_vec[shard_idx as usize].write().unwrap())
        .collect()
}


// Shard data grouped in a vector index
pub struct ShardData {
    // Bloomfilter
    pub(crate) filter: BitVec,

    // Metadata
    pub(crate) key_count: u64,
    pub(crate) bloom_length: u64,
    pub(crate) bloom_length_mult: u32,
    pub(crate) hash_family_size: u32,


}

impl Default for ShardData {
    fn default() ->  Self {
        Self::new(BLOOM_STARTING_LENGTH, 0, BLOOM_STARTING_MULT, BLOOM_HASH_FAMILY_SIZE)
    }
}

impl ShardData {
    fn new(filter_starting_len: u64, key_count: u64, filter_starting_len_mult: u32, filter_starting_hash_family: u32) -> Self {
        let filter = bitvec![0; filter_starting_len as usize];
        let key_count = key_count;
        let bloom_length = filter_starting_len;
        let bloom_length_mult = filter_starting_len_mult;
        let hash_family_size = filter_starting_hash_family;
        
        ShardData { filter, key_count, bloom_length, bloom_length_mult, hash_family_size }
    }
}


