use std::{collections::{HashSet, VecDeque}, fs::metadata, sync::{atomic::AtomicBool, Arc, RwLock, RwLockWriteGuard}, time::Duration};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::{Lazy};


use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, ARRAY_SHARDS, BLOOM_HASH_FAMILY_SIZE, BLOOM_STARTING_LENGTH, BLOOM_STARTING_MULT}, utils::{concurrecy_init, CollisionResult, FilterType}};

pub static GLOBAL_PBF: Lazy<PerfectBloomFilter> = Lazy::new(|| {
    PerfectBloomFilter::default()
});









pub struct PerfectBloomFilter {
    pub(crate) outer_filter: ShardVector,
    pub(crate) inner_filter: ShardVector,
}

impl Default for PerfectBloomFilter {
    fn default() -> Self {
        Self::new()
    }
}


pub struct ShardVector {
    pub shard_vector: Arc<Vec<RwLock<ShardData>>>,
}

impl Default for ShardVector {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardVector {
    fn new() -> Self {
        let shard_vector = (0..ARRAY_SHARDS)
            .map(|_| RwLock::new(ShardData::new()))
            .collect::<Vec<_>>();

        let shard_arc = Arc::new(shard_vector);

        ShardVector { shard_vector: shard_arc }
    }
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
        Self::new()
    }
}

impl ShardData {
    fn new() -> Self {
        let filter = bitvec![0; BLOOM_STARTING_LENGTH as usize];
        let key_count = 0;
        let bloom_length = BLOOM_STARTING_LENGTH;
        let bloom_length_mult = BLOOM_STARTING_MULT;
        let hash_family_size = BLOOM_HASH_FAMILY_SIZE;
        
        ShardData { filter, key_count, bloom_length, bloom_length_mult, hash_family_size }
    }
}

impl PerfectBloomFilter {
    fn new() -> Self {
        let outer_filter = ShardVector::default();
        let inner_filter = ShardVector::default();

        PerfectBloomFilter {
            outer_filter: outer_filter,
            inner_filter: inner_filter,
        }
    }

    pub fn system() -> &'static PerfectBloomFilter{
        &*GLOBAL_PBF
    }

    pub fn contains(&self, key: &str) -> Result<bool> {
        let outer = Self::existence_check(key, &FilterType::Outer)?;
        let inner = Self::existence_check(key, &FilterType::Inner)?;

       Ok(outer & inner)
    }

    pub fn insert(&self, key: &str) -> Result<()> {
        Self::insert_key(key, &FilterType::Outer)?;
        Self::insert_key(key, &FilterType::Inner)?;

        Ok(())
    }


    fn existence_check(key: &str, filter_type: &FilterType) -> Result<bool>{
        let shards = array_sharding_hash(key, filter_type)?;

        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
        let collision_results = bloom_check(&shards_hashes, filter_type)?;

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

    fn insert_key(key: &str, filter_type: &FilterType) -> Result<()>{
        let shards = array_sharding_hash(key, filter_type)?;
        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
    
        bloom_insert(&shards_hashes, filter_type)?;




       
        Ok(())
    }
}
