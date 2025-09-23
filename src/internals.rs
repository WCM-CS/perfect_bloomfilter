use std::sync::{Arc, RwLock, RwLockWriteGuard};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::{Lazy, OnceCell};

use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, ARRAY_SHARDS, BLOOM_HASH_FAMILY_SIZE, BLOOM_STARTING_LENGTH, BLOOM_STARTING_MULT}, utils::{concurrecy_init, CollisionResult, FilterType}};

//pub static GLOBAL_PBF: OnceCell<PerfectBloomFilter> = OnceCell::new();

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
    pub shard_vector: Arc<Vec<ShardData>>,
}

impl Default for ShardVector {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardVector {
    fn new() -> Self {
        let shard_vector = (0..ARRAY_SHARDS)
            .map(|_| ShardData::new())
            .collect::<Vec<_>>();

        let shard_arc = Arc::new(shard_vector);
        
        ShardVector { shard_vector: shard_arc }
    }
}

// Shard data grouped in a vector index
pub struct ShardData {
    // Bloomfilter
    pub(crate) filter: RwLock<BitVec>,

    // Metadata
    pub(crate) key_count: RwLock<u64>,
    pub(crate) bloom_length: RwLock<u64>,
    pub(crate) bloom_length_mult: RwLock<u32>,
    pub(crate) hash_family_size: RwLock<u32>,

    // IO Cache
    pub(crate) key_cache: RwLock<Vec<String>>,

}

impl Default for ShardData {
    fn default() ->  Self {
        Self::new()
    }
}

impl ShardData {
    fn new() -> Self {
        let filter = RwLock::new(bitvec![0; BLOOM_STARTING_LENGTH as usize]);
        let key_count = RwLock::new(0);
        let bloom_length = RwLock::new(BLOOM_STARTING_LENGTH);
        let bloom_length_mult = RwLock::new(BLOOM_STARTING_MULT);
        let hash_family_size = RwLock::new(BLOOM_HASH_FAMILY_SIZE);
        let key_cache = RwLock::new(Vec::new());
        
        ShardData { filter, key_count, bloom_length, bloom_length_mult, hash_family_size, key_cache }
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

     pub fn system_init() -> &'static PerfectBloomFilter{   
        // Array shards, Low 2^11, med 2^12, high 2^13, defauolt med



        &*GLOBAL_PBF
    }

    pub fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let outer = Self::contains_insert(key, &FilterType::Outer)?;
        let inner = Self::contains_insert(key, &FilterType::Inner)?;
      
        Ok(inner & outer)
    }

    pub fn contains(&self, key: &str) -> Result<bool> {
        let outer = Self::filters_contain(key, &FilterType::Outer)?;
        let inner = Self::filters_contain(key, &FilterType::Inner)?;

        Ok(outer & inner)
    }

    pub fn insert(&self, key: &str) -> Result<()> {
        Self::filters_insert(key, &FilterType::Outer)?;
        Self::filters_insert(key, &FilterType::Inner)?;

        Ok(())
    }

    fn contains_insert(key: &str, filter_type: &FilterType) -> Result<bool>{
        let shards = array_sharding_hash(key, filter_type)?;
        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
        let collision_results = bloom_check(&shards_hashes, filter_type)?;

        let exists = match collision_results {
            CollisionResult::Zero => {
                bloom_insert(&shards_hashes, filter_type);
                false
            },
            CollisionResult::Partial(_) => {
                bloom_insert(&shards_hashes, filter_type);
                false
            }
            CollisionResult::Complete(_, _) => {
                true
            }
            CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };
        
        Ok(exists)
    }


    fn filters_contain(key: &str, filter_type: &FilterType) -> Result<bool> {
        let shards = array_sharding_hash(key, filter_type)?;
        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
        let collision_results = bloom_check(&shards_hashes, filter_type)?;

        let exists = match collision_results {
            CollisionResult::Zero => {
                false
            },
            CollisionResult::Partial(_) => {
                false
            }
            CollisionResult::Complete(_, _) => {
                true
            }
            CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };
        
        Ok(exists)
    }

    fn filters_insert(key: &str, filter_type: &FilterType) -> Result<()> {
        let shards = array_sharding_hash(key, filter_type)?;
        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
        let collision_results = bloom_check(&shards_hashes, filter_type)?;

        match collision_results {
            CollisionResult::Zero => {
                bloom_insert(&shards_hashes, filter_type);
            },
            CollisionResult::Partial(_) => {
                bloom_insert(&shards_hashes, filter_type);
            }
            CollisionResult::Complete(_, _) => {
            }
            CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };

        Ok(())
    }

}







fn lock_shard_state<'a>(shard: u32, filter_type: FilterType) -> Result<(
    RwLockWriteGuard<'a, u64>, 
    RwLockWriteGuard<'a, u32>, 
    RwLockWriteGuard<'a, u64>, 
    RwLockWriteGuard<'a, BitVec>
)> {
    let(len, len_mult, len_count, filter) = match filter_type {

        FilterType::Outer => {
            let len = GLOBAL_PBF.outer_filter.shard_vector[shard as usize].bloom_length.write().unwrap();
            let len_mult = GLOBAL_PBF.outer_filter.shard_vector[shard as usize].bloom_length_mult.write().unwrap();
            let len_count = GLOBAL_PBF.outer_filter.shard_vector[shard as usize].key_count.write().unwrap();
            let filter = GLOBAL_PBF.outer_filter.shard_vector[shard as usize].filter.write().unwrap();
            (len, len_mult, len_count, filter)
        },
        FilterType::Inner => {
            let len = GLOBAL_PBF.inner_filter.shard_vector[shard as usize].bloom_length.write().unwrap();
            let len_mult = GLOBAL_PBF.inner_filter.shard_vector[shard as usize].bloom_length_mult.write().unwrap();
            let len_count = GLOBAL_PBF.inner_filter.shard_vector[shard as usize].key_count.write().unwrap();
            let filter =GLOBAL_PBF.inner_filter.shard_vector[shard as usize].filter.write().unwrap();
            (len, len_mult, len_count, filter)
        },
    };

    Ok((len, len_mult, len_count, filter))
}


/*

pub struct Config {
    pub data_volume: DataVolume,
    pub data_correctness: DataCorrectness,
    pub system_threads: SystemThreads,
    pub hash_functions: HashFunction,

}

impl Default for Config {
    fn default() -> Self {
        let threads = concurrecy_init().unwrap(); // cahe to allow enum options into it
        Config { 
            data_volume: DataVolume::Medium, 
            data_correctness: DataCorrectness::Medium, 
            system_threads: SystemThreads::Medium(threads), 
            hash_functions: HashFunction::Xxh3
        }
    }
}

#[repr(u32)]
pub enum DataVolume { // Shard count
    High = 1u32 << 13,
    Medium = 1u32 << 12,
    Low = 1u32 << 11,
}

pub enum DataCorrectness {
    High, // Spawn triple cascade 
    Medium, // Spawn double cascade 
    Low // spawn single multidimentional bloomfilter
}

pub enum SystemThreads {
    Custom(usize), // Use custom user param 
    High(usize), // Use 80% system threads by default 
    Medium(usize), // Use 60% system threads by default 
    Low(usize), // Use 40% system threads by default
}

#[derive(Clone)]
pub enum HashFunction {
    Murmur3, // Excellent compatability, fast: murmur3::murmur3_x64_128
    Xxh3, // Default, Great compatability, very fast: xxhash_rust::xxh3::xxh3_128_with_seed
    CityHash, // Great compatability, fast: fasthash::city::hash128_with_seed 
    FarmHash, // Good compatability, very fast: fasthash::farm::hash128_with_seed
    GxHash, // Okay compatability, very fast: gxhash::gxhash128
    MeowHash, // Low Compatability, Fastest: mewohash::
}



*/


