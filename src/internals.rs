use std::{collections::{HashSet, VecDeque}, fs::metadata, sync::{atomic::AtomicBool, Arc, RwLock, RwLockWriteGuard}, time::Duration};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::{Lazy};


use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, ARRAY_SHARDS, BLOOM_HASH_FAMILY_SIZE, BLOOM_STARTING_LENGTH, BLOOM_STARTING_MULT}, io::{drain_cache, insert_into_cache}, rehash::metadata_computation, utils::{concurrecy_init, CollisionResult, FilterType}};

pub static GLOBAL_PBF: Lazy<PerfectBloomFilter> = Lazy::new(|| {
    PerfectBloomFilter::default()
});

pub static ACTIVE_STATE: Lazy<ActiveState> = Lazy::new(|| {
    ActiveState::default()
});

pub static REHASH_QUEUE: Lazy<RehashQueue> = Lazy::new(|| {
    RehashQueue::default()
});


pub struct RehashQueue {
    pub outer_queue: Arc<RwLock<VecDeque<u32>>>, 
    pub inner_queue: Arc<RwLock<VecDeque<u32>>>, 
    pub outer_lookup_list: Arc<RwLock<HashSet<u32>>>, 
    pub inner_lookup_list: Arc<RwLock<HashSet<u32>>>, 
}   

impl Default for RehashQueue {
    fn default() -> Self {
        let outer_queue = Arc::new(RwLock::new(VecDeque::new()));
        let inner_queue = Arc::new(RwLock::new(VecDeque::new()));

        let outer_lookup_list = Arc::new(RwLock::new(HashSet::new()));
        let inner_lookup_list = Arc::new(RwLock::new(HashSet::new()));

        Self { outer_queue, inner_queue, outer_lookup_list, inner_lookup_list }
    }
}



pub struct ActiveState {
    pub outer_drain_state: Arc<Vec<AtomicBool>>,
    pub inner_drain_state: Arc<Vec<AtomicBool>>,

    pub outer_rehash_state: Arc<Vec<AtomicBool>>,
    pub inner_rehash_state: Arc<Vec<AtomicBool>>,
}

impl Default for ActiveState {
    fn default() -> Self {
        let arc_outer_drain = Arc::new((0..ARRAY_SHARDS).map(|_| AtomicBool::new(false)).collect());
        let arc_outer_rehash = Arc::new((0..ARRAY_SHARDS).map(|_| AtomicBool::new(false)).collect());
        let arc_inner_drain = Arc::new((0..ARRAY_SHARDS).map(|_| AtomicBool::new(false)).collect());
        let arc_inner_rehash = Arc::new((0..ARRAY_SHARDS).map(|_| AtomicBool::new(false)).collect());

        Self {
            outer_drain_state: arc_outer_drain,
            outer_rehash_state: arc_outer_rehash,
            inner_drain_state: arc_inner_drain,
            inner_rehash_state: arc_inner_rehash,
        }
    }
}









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
    pub(crate) output_cache: RwLock<Vec<String>>,

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
        let output_cache = RwLock::new(Vec::new());
        
        ShardData { filter, key_count, bloom_length, bloom_length_mult, hash_family_size, output_cache}
    }
}

impl PerfectBloomFilter {
    fn new() -> Self {
        let sys_threads = concurrecy_init().unwrap();
        let rehash_threads = sys_threads - 2;

        std::thread::spawn(move || {
            loop {
                let _ = drain_cache(FilterType::Outer);
            }
        });

        std::thread::spawn(move || {
            loop {
                let _ = drain_cache(FilterType::Inner);
            }
        });





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
        let shard_idx = shards_hashes.keys().copied().collect::<Vec<_>>();

        loop {
            let (rehash_state, drain_state) = match filter_type {
                FilterType::Outer => (&ACTIVE_STATE.outer_rehash_state, &ACTIVE_STATE.outer_drain_state),
                FilterType::Inner => (&ACTIVE_STATE.inner_rehash_state, &ACTIVE_STATE.inner_drain_state)
            };

            let is_rehashing = shard_idx.iter().any(|&shard| {
                rehash_state[shard as usize].load(std::sync::atomic::Ordering::SeqCst)
            });

            if !is_rehashing {
                shard_idx.iter().for_each(|shard| {
                    drain_state[*shard as usize].store(true, std::sync::atomic::Ordering::SeqCst);
                });

                bloom_insert(&shards_hashes, filter_type)?;
                insert_into_cache(key, filter_type, &shard_idx)?;

                metadata_computation(filter_type, &shard_idx);

                shard_idx.iter().for_each(|shard| {
                    drain_state[*shard as usize].store(false, std::sync::atomic::Ordering::SeqCst);
                });

                break;
            }

            std::thread::sleep(Duration::from_millis(100));
        }


       
        Ok(())
    }
}







fn lock_shard_state<'a>(shard: u32, filter_type: FilterType) -> Result<(
    RwLockWriteGuard<'a, BitVec>, // filter
    RwLockWriteGuard<'a, u64>, // key count
    RwLockWriteGuard<'a, u64>, // length
    RwLockWriteGuard<'a, u32>, // len mult
    RwLockWriteGuard<'a, u32>, // family size
    RwLockWriteGuard<'a, Vec<String>> // cache
)> {

    let shard_vec = match filter_type {
        FilterType::Outer => &GLOBAL_PBF.outer_filter.shard_vector[shard as usize],
        FilterType::Inner => &GLOBAL_PBF.outer_filter.shard_vector[shard as usize],
    };

    let filter = shard_vec.filter.write().unwrap();
    let key_count = shard_vec.key_count.write().unwrap();
    let len = shard_vec.bloom_length.write().unwrap();
    let len_mult = shard_vec.bloom_length_mult.write().unwrap();
    let family_size = shard_vec.hash_family_size.write().unwrap();
    let cache_output = shard_vec.output_cache.write().unwrap();
    
    Ok((filter, key_count, len, len_mult, family_size, cache_output))
}

