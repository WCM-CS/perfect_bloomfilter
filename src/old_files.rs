use anyhow::{Result, anyhow};
use bitvec::{bitvec, vec::BitVec};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet}, fs, io::{self, BufRead, Cursor, Write}, path::Path, sync::{Arc, Mutex, RwLock}, thread::{self}, time::Duration
};
use threadpool::ThreadPool;
use csv::Writer;

pub const HASH_SEED_SELECTION: [u32; 6] = [9, 223, 372, 530, 775, 954];


pub enum FilterType {
    Outer,
    Inner,
}

struct PerfectBloomFilter {
    outer_filter: Arc<OuterBlooms>,
    inner_filter: Arc<InnerBlooms>,
}

impl PerfectBloomFilter {
    pub fn new() -> Result<Self> {
        let outer_filter = Arc::new(OuterBlooms::default());
        let inner_filter = Arc::new(InnerBlooms::default());

        let weak_drain_outer_filter = Arc::downgrade(&Arc::clone(&outer_filter));
        let weak_drain_inner_filter = Arc::downgrade(&Arc::clone(&inner_filter));
        let weak_resize_outer_filter = Arc::downgrade(&Arc::clone(&outer_filter));
        let weak_resize_inner_filter = Arc::downgrade(&Arc::clone(&inner_filter));


        let (drain_threads, rehash_threads) = concurrecy_init()?;
        let drain_pool = ThreadPool::new(drain_threads);
        let rehash_pool = ThreadPool::new(rehash_threads);

        drain_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(2));
            
                
                let data = {
                    if let Some(strong_outer_filter) = weak_drain_outer_filter.upgrade() {
                        let mut locked_outer_filter = strong_outer_filter.outer_disk_cache.lock().unwrap();
                        std::mem::take(&mut *locked_outer_filter)
                    } else { continue }
                };

                if let Err(e) = write_disk_io_cache(data, FilterType::Outer) {
                    tracing::warn!("Failed to write keys from cache outer blooms to disk: {}", e);
                }
            }
        });

        drain_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(2));

             
                let data = {
                    if let Some(strong_inner_filter) = weak_drain_inner_filter.upgrade() {
                        let mut locked_inner_filter = strong_inner_filter.inner_disk_cache.lock().unwrap();
                        std::mem::take(&mut *locked_inner_filter)
                    } else { continue }
                };

                if let Err(e) = write_disk_io_cache(data, FilterType::Inner) {
                    tracing::warn!("Failed to write keys from cache inner blooms to disk: {}", e);
                }
            }
        });

        
        rehash_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(2));

                let shards = {
                    if let Some(strong_outer_filter) = weak_resize_outer_filter.upgrade() {
                        let mut locked_outer_filter = strong_outer_filter.outer_rehash_list.lock().unwrap();
                        std::mem::take(&mut *locked_outer_filter)
                    } else { continue }
                };

                if !shards.is_empty() {
                    let locked_filter = weak_resize_outer_filter.upgrade().unwrap();
                    match locked_filter.rehash(shards.clone()) {
                        Ok(_) => tracing::info!("Rehashed inner shard: {:?}", shards),
                        Err(_) => todo!(),
                    }
                }
            }
        });

        rehash_pool.execute(move || {
            loop {
                thread::sleep(Duration::from_secs(2));

                let shards = {
                    if let Some(strong_inner_filter) = weak_resize_inner_filter.upgrade() {
                        let mut locked_inner_filter = strong_inner_filter.inner_rehash_list.lock().unwrap();
                        std::mem::take(&mut *locked_inner_filter)
                    } else { continue }
                };

                if !shards.is_empty() {
                    let locked_filter = weak_resize_inner_filter.upgrade().unwrap();
                    match locked_filter.rehash(shards.clone()) {
                        Ok(_) => tracing::info!("Rehashed inner shard: {:?}", shards),
                        Err(_) => todo!(),
                    }
                }

            }
        });


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

    pub async fn async_contains_insert(&mut self, key: &str) -> Result<bool> {
        let outer_res = self.outer_filter.contains_and_insert(key)?;
        let inner_res = self.inner_filter.contains_and_insert(key)?;

        Ok(outer_res && inner_res)
    }

    pub fn metadata_dump(&self) -> Result<()>{
        let outer_meta = self.outer_filter.outer_metadata.read().unwrap();
        let inner_meta = self.inner_filter.inner_metadata.read().unwrap();
        let path = Path::new("./metadata");
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let file_path = path.join("metadata.csv");
        let mut wtr = Writer::from_path(file_path)?;

        // Write CSV headers
        wtr.write_record(&[
            "Type", "Shard", "KeyCount", "BitLength", "BitLengthMult"
        ])?;

        // Write Outer metadata rows
        for (&shard, &key_count) in &outer_meta.blooms_key_count {
            let bit_len = outer_meta.bloom_bit_length.get(&shard).copied().unwrap_or(0);
            let mult = outer_meta.bloom_bit_length_mult.get(&shard).copied().unwrap_or(0);
            
            wtr.write_record(&[
                "Outer",
                &shard.to_string(),
                &key_count.to_string(),
                &bit_len.to_string(),
                &mult.to_string(),
            ])?;
        }

        // Write Inner metadata rows
        for (&shard, &key_count) in &inner_meta.blooms_key_count {
            let bit_len = inner_meta.bloom_bit_length.get(&shard).copied().unwrap_or(0);
            let mult = inner_meta.bloom_bit_length_mult.get(&shard).copied().unwrap_or(0);
            
            wtr.write_record(&[
                "Inner",
                &shard.to_string(),
                &key_count.to_string(),
                &bit_len.to_string(),
                &mult.to_string(),
            ])?;
        }

        wtr.flush()?;
        Ok(())
    }
    
}

pub struct OuterBlooms {
    outer_shards: Arc<RwLock<OuterShardArray>>,
    outer_metadata: Arc<RwLock<OuterMetaData>>,
    outer_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>, //cache struct
    outer_rehash_list: Arc<Mutex<HashSet<u32>>>,
    outer_rehash_cache: Arc<Mutex<HashMap<u32, HashSet<String>>>>, // insert intot here wehn a rehash is hapenning for a given shard
    outer_shards_active_rehashing: Arc<RwLock<HashSet<u32>>>, // shards being rehashed,when true then insert into the cache

}

impl Default for OuterBlooms {
    fn default() -> Self {
        Self::new()
    }
}

impl OuterBlooms {
    pub fn new() -> Self {
        let outer_shards = Arc::new(RwLock::new(OuterShardArray::default()));
        let outer_metadata = Arc::new(RwLock::new(OuterMetaData::default()));
        let outer_disk_cache = Arc::new(Mutex::new(HashMap::new()));
        let outer_rehash_list = Arc::new(Mutex::new(HashSet::new()));
        let outer_rehash_cache = Arc::new(Mutex::new(HashMap::new()));
        let outer_shards_active_rehashing = Arc::new(RwLock::new(HashSet::new()));

        Self {
            outer_shards,
            outer_metadata,
            outer_disk_cache,
            outer_rehash_list,
            outer_rehash_cache,
            outer_shards_active_rehashing
        }
    }
}

pub struct InnerBlooms {
    inner_shards: Arc<RwLock<InnerShardArray>>,
    inner_metadata: Arc<RwLock<InnerMetaData>>,
    inner_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>,
    inner_rehash_list: Arc<Mutex<HashSet<u32>>>,
    inner_rehash_cache: Arc<Mutex<HashMap<u32, HashSet<String>>>>, // insert intot here wehn a rehash is hapenning for a given shard
    inner_shards_active_rehashing: Arc<RwLock<HashSet<u32>>>, // shards being rehashed,when true then insert into the cache

}

impl Default for InnerBlooms {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerBlooms {
    pub fn new() -> Self {
        let inner_shards = Arc::new(RwLock::new(InnerShardArray::default()));
        let inner_metadata = Arc::new(RwLock::new(InnerMetaData::default()));
        let inner_disk_cache = Arc::new(Mutex::new(HashMap::new()));
        let inner_rehash_list = Arc::new(Mutex::new(HashSet::new()));
        let inner_rehash_cache = Arc::new(Mutex::new(HashMap::new()));
        let inner_shards_active_rehashing = Arc::new(RwLock::new(HashSet::new()));

        Self {
            inner_shards,
            inner_metadata,
            inner_disk_cache,
            inner_rehash_list,
            inner_rehash_cache,
            inner_shards_active_rehashing
        }
    }
}

pub trait BloomFilter {
    type CollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool>;
    fn bloom_hash(&self, vector_partitions: &[u32], key: &str) -> Result<HashMap<u32, Vec<u64>>>;
    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> Result<CollisionResult>;
    fn bloom_insert(&self, outer_hashed: &HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str);
    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>>;
}   

impl BloomFilter for OuterBlooms {
    type CollisionResult = CollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let outer_shard_slots = self.array_partition_hash(key)?; // get the outer blooms two shard idx

        let active_rehashing_shards = {
            let locked_shard = self.outer_shards_active_rehashing.read().unwrap();
            let active_rehashing_shards: Vec<u32> = outer_shard_slots
                .iter()
                .filter(|shard| locked_shard.contains(shard))
                .copied()
                .collect();
            Some(active_rehashing_shards)
        };


        let outer_blooms_idx = self.bloom_hash(&outer_shard_slots, key)?;
        let outer_collision_result = self.bloom_check(&outer_blooms_idx, &active_rehashing_shards)?;

       let outer_exists = match outer_collision_result {
            CollisionResult::Zero => {
                self.bloom_insert(&outer_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &outer_shard_slots)?;
                false
            }
            CollisionResult::PartialMinor(_) => {
                self.bloom_insert(&outer_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &outer_shard_slots)?;
                false
            },
            CollisionResult::PartialMajor(s1, s2) => {
                self.bloom_insert(&outer_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &outer_shard_slots)?;
                let _ = self.rehash_list_update(&vec![s1, s2]);
                false
            },
            CollisionResult::Complete(_, _, _) => {
                true
            },
            CollisionResult::Error => {
               tracing::warn!("unexpected issue with collision result for key: {key}");
               return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };

        

        Ok(outer_exists)
    }

    fn bloom_hash(
        &self,
        vector_partitions: &[u32],
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.outer_metadata.read().unwrap();

        for &vector_slot in vector_partitions {
            let bloom_length = match inner.bloom_bit_length.get(&vector_slot) {
                // GET THE BLOOMIES LENGTH PER THE SHARD IDX
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[0])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[1])?;

            for idx in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128;
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> Result<CollisionResult> {
        // return them is in the cache
        let active_rehashing_shards = match active_rehashing_shards {
            Some(shard_list) => shard_list,
            None => &vec![0]
        };
        

        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        let inner = self.outer_shards.read().unwrap();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| inner.data[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        let collision_result = check_collision(&collision_map)?;

        let result = match collision_result {
            CollisionResult::Zero => {
                if active_rehashing_shards.len() == 3 {
                    CollisionResult::Complete(active_rehashing_shards[0],active_rehashing_shards[1], active_rehashing_shards[2] )
                } else if active_rehashing_shards.len() == 2 {
                    CollisionResult::PartialMajor(active_rehashing_shards[0], active_rehashing_shards[1])
                } else if active_rehashing_shards.len() == 1 {
                    CollisionResult::PartialMinor(active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::Zero
                } else {
                    CollisionResult::Error
                }
            },
            CollisionResult::PartialMinor(s1) => {
                if active_rehashing_shards.len() == 2 {
                    CollisionResult::Complete(s1, active_rehashing_shards[0], active_rehashing_shards[1])
                } else if active_rehashing_shards.len() == 1 {
                    CollisionResult::PartialMajor(s1, active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::PartialMinor(s1)
                } else {
                    CollisionResult::Error
                }
            }
            CollisionResult::PartialMajor(s1, s2) => {
                if active_rehashing_shards.len() == 1 {
                    CollisionResult::Complete(s1, s2, active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::PartialMajor(s1, s2)
                } else {
                    CollisionResult::Error
                }
            },
            CollisionResult::Complete(s1, s2, s3) => {
                CollisionResult::Complete(s1, s2, s3)
            },
            CollisionResult::Error => {
                CollisionResult::Error
            }
        };


        Ok(collision_result)
    }

    fn bloom_insert(&self, outer_hashed: &HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str) {
        if let Some(active_shards) = active_rehashing_shards.clone() {
            for (vector_index, hashes) in outer_hashed {
                if active_shards.contains(vector_index) {
                    let _ = self.rehash_cache_insert( vec![*vector_index], key); // push to rehash cache dont mess with metadata
                } else {
                    let mut outer = self.outer_shards.write().unwrap();
                    let mut outer_met = self.outer_metadata.write().unwrap();
                    
                    hashes.iter().for_each(|&bloom_index| {
                        outer.data[*vector_index as usize].set(bloom_index as usize, true)
                    });

                    outer_met
                        .blooms_key_count
                        .entry(*vector_index)
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                
                }
            } 
        } else {
            let mut outer = self.outer_shards.write().unwrap();
            let mut outer_met = self.outer_metadata.write().unwrap();
            
            for (vector_index, hashes) in outer_hashed {
                hashes.iter().for_each(|&bloom_index| {
                    outer.data[*vector_index as usize].set(bloom_index as usize, true);
                });
                outer_met.blooms_key_count
                    .entry(*vector_index)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }   
        }
    }

    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        // two shards: jump hash p1 and use n/2 mod shard len
        // three shards: jump hash p1, use p1+n/3 mod n and p1+2n/3 mod n
        /*
         let shard_len: u32 = STATIC_VECTOR_LENGTH_OUTER as u32;

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[2])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;
        let shard_size = shard_len / 2;

        let p1 = jump_hash_partition(low ^ high, shard_len)?;
        let p2 = (p1 + shard_size) % shard_len;

        debug_assert!(p1 != p2, "Partitions must be unique");

        Ok(vec![p1, p2])
         */

        let shard_len: u32 = STATIC_VECTOR_LENGTH_OUTER as u32;

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[2])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;
        let shard_size = shard_len / 3;

        let p1 = jump_hash_partition(high ^ low, shard_len)?;
        let p2 = (p1 + shard_size) % shard_len;
        let p3 = (p1 + (2 * shard_size)) % shard_len;

        debug_assert!(
            p1 != p2 && p1 != p3 && p2 != p3,
            "Partitions must be unique"
        );

        Ok(vec![p1, p2, p3])

       
    }
}

impl BloomFilter for InnerBlooms {
    type CollisionResult = CollisionResult;

    fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let inner_shard_slots = self.array_partition_hash(key)?; // get the outer blooms three shard idx

        let active_rehashing_shards = {
            let locked_shard = self.inner_shards_active_rehashing.read().unwrap();
            let active_rehashing_shards: Vec<u32> = inner_shard_slots
                .iter()
                .filter(|shard| locked_shard.contains(shard))
                .copied()
                .collect();
            Some(active_rehashing_shards)
        };

        let inner_blooms_idx = self.bloom_hash(&inner_shard_slots, key)?;
        let inner_collision_result = self.bloom_check(&inner_blooms_idx, &active_rehashing_shards)?;


        let inner_exists = match inner_collision_result {
            CollisionResult::Zero => {
                self.bloom_insert(&inner_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &inner_shard_slots)?;
                false
            }
            CollisionResult::PartialMinor(_) => {
                self.bloom_insert(&inner_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &inner_shard_slots)?;
                false
            },
            CollisionResult::PartialMajor(s1, s2) => {
                self.bloom_insert(&inner_blooms_idx, active_rehashing_shards, key);
                self.insert_disk_io_cache(key, &inner_shard_slots)?;
                let _ = self.rehash_list_update(&vec![s1, s2]);
                false
            },
            CollisionResult::Complete(_, _, _) => {
                true
            },
            CollisionResult::Error => {
               tracing::warn!("unexpected issue with collision result for key: {key}");
               return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };

        

        Ok(inner_exists)
    }

    fn bloom_hash(
        &self,
        vector_partitions: &[u32],
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.inner_metadata.read().unwrap();

        for &vector_slot in vector_partitions {
            let bloom_length = match inner.bloom_bit_length.get(&vector_slot) {
                // GET THE BLOOMIES LENGTH PER THE SHARD IDX
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[4])?;

            for idx in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128; // potentially add a prime number here to ensure its a solid dirtibution in the shards
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn bloom_check(&self, map: &HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> Result<CollisionResult> {
        // return them is in the cache
        let active_rehashing_shards = match active_rehashing_shards {
            Some(shard_list) => shard_list,
            None => &vec![0]
        };
        

        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        let inner = self.inner_shards.read().unwrap();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| inner.data[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        let collision_result = check_collision(&collision_map)?;

        let result = match collision_result {
            CollisionResult::Zero => {
                if active_rehashing_shards.len() == 3 {
                    CollisionResult::Complete(active_rehashing_shards[0],active_rehashing_shards[1], active_rehashing_shards[2] )
                } else if active_rehashing_shards.len() == 2 {
                    CollisionResult::PartialMajor(active_rehashing_shards[0], active_rehashing_shards[1])
                } else if active_rehashing_shards.len() == 1 {
                    CollisionResult::PartialMinor(active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::Zero
                } else {
                    CollisionResult::Error
                }
            },
            CollisionResult::PartialMinor(s1) => {
                if active_rehashing_shards.len() == 2 {
                    CollisionResult::Complete(s1, active_rehashing_shards[0], active_rehashing_shards[1])
                } else if active_rehashing_shards.len() == 1 {
                    CollisionResult::PartialMajor(s1, active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::PartialMinor(s1)
                } else {
                    CollisionResult::Error
                }
            }
            CollisionResult::PartialMajor(s1, s2) => {
                if active_rehashing_shards.len() == 1 {
                    CollisionResult::Complete(s1, s2, active_rehashing_shards[0])
                } else if active_rehashing_shards.len() == 0 {
                    CollisionResult::PartialMajor(s1, s2)
                } else {
                    CollisionResult::Error
                }
            },
            CollisionResult::Complete(s1, s2, s3) => {
                CollisionResult::Complete(s1, s2, s3)
            },
            CollisionResult::Error => {
                CollisionResult::Error
            }
        };

        Ok(collision_result)
    }

    fn bloom_insert(&self, inner_hashed: &HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str) {
        if let Some(active_shards) = active_rehashing_shards.clone() {
            for (vector_index, hashes) in inner_hashed {
                if active_shards.contains(vector_index) {
                    let _ = self.rehash_cache_insert( vec![*vector_index], key); // push to rehash cache dont mess with metadata
                } else {
                    let mut outer = self.inner_shards.write().unwrap();
                    let mut outer_met = self.inner_metadata.write().unwrap();
                    
                    hashes.iter().for_each(|&bloom_index| {
                        outer.data[*vector_index as usize].set(bloom_index as usize, true)
                    });

                    outer_met
                        .blooms_key_count
                        .entry(*vector_index)
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                
                }
            } 
        } else {
            let mut outer = self.inner_shards.write().unwrap();
            let mut outer_met = self.inner_metadata.write().unwrap();
            
            for (vector_index, hashes) in inner_hashed {
                hashes.iter().for_each(|&bloom_index| {
                    outer.data[*vector_index as usize].set(bloom_index as usize, true);
                });
                outer_met.blooms_key_count
                .entry(*vector_index)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            }   
        }
    }

    fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        // two shards: jump hash p1 and use n/2 mod shard len
        // three shards: jump hash p1, use p1+n/3 mod n and p1+2n/3 mod n

        let shard_len: u32 = STATIC_VECTOR_LENGTH_INNER as u32;

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[5])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;
        let shard_size = shard_len / 3;

        let p1 = jump_hash_partition(high ^ low, shard_len)?;
        let p2 = (p1 + shard_size) % shard_len;
        let p3 = (p1 + (2 * shard_size)) % shard_len;

        debug_assert!(
            p1 != p2 && p1 != p3 && p2 != p3,
            "Partitions must be unique"
        );

        Ok(vec![p1, p2, p3])
    }

}

#[derive(Debug)]
struct OuterShardArray {
    data: [BitVec; STATIC_VECTOR_LENGTH_OUTER as usize],
}

impl Default for OuterShardArray {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; OUTER_BLOOM_DEFAULT_LENGTH as usize];
        let data = std::array::from_fn(|_| empty_bitvec.clone());
        OuterShardArray { data }
    }
}

#[derive(Debug)]
struct InnerShardArray {
    data: [BitVec; STATIC_VECTOR_LENGTH_INNER as usize],
}

impl Default for InnerShardArray {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; INNER_BLOOM_DEFAULT_LENGTH as usize];
        let data = std::array::from_fn(|_| empty_bitvec.clone());
        InnerShardArray { data }
    }
}

#[derive(Debug)]
struct OuterMetaData {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
    bloom_bit_length_mult: HashMap<u32, u32>,
    bloom_rehashes_count: u32,
}

impl Default for OuterMetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl OuterMetaData {
    pub fn new() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_OUTER as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_OUTER as usize);
        let mut bit_length_mult: HashMap<u32, u32> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_OUTER as usize);

        for partition in 0..STATIC_VECTOR_LENGTH_OUTER {
            key_count.insert(partition, 0);
            bit_length.insert(partition, OUTER_BLOOM_DEFAULT_LENGTH); // the default outer bloomies len should be larger than the inner since the shard arr len is smaller, reduces cpu load via less rehashes on init
            bit_length_mult.insert(partition, OUTER_BLOOM_STARTING_MULT);
        }

        Self {
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
            bloom_bit_length_mult: bit_length_mult,
            bloom_rehashes_count: 0
        }
    }
}

#[derive(Debug)]
struct InnerMetaData {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
    bloom_bit_length_mult: HashMap<u32, u32>,
    bloom_rehashes_count: u32,
}
impl Default for InnerMetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerMetaData {
    pub fn new() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_INNER as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_INNER as usize);
        let mut bit_length_mult: HashMap<u32, u32> = HashMap::with_capacity(STATIC_VECTOR_LENGTH_INNER as usize);

        for partition in 0..STATIC_VECTOR_LENGTH_INNER {
            key_count.insert(partition, 0);
            bit_length.insert(partition, INNER_BLOOM_DEFAULT_LENGTH); // the default outer bloomies len should be larger than the inner since the shard arr len is smaller, reduces cpu load via less rehashes on init
            bit_length_mult.insert(partition, INNER_BLOOM_STARTING_MULT);
        }

        Self {
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
            bloom_bit_length_mult: bit_length_mult,
            bloom_rehashes_count: 0
        }
    }
}

// JumpConsistentHash function, rewritten from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
fn jump_hash_partition(key: u64, buckets: u32) -> Result<u32> {
    let mut b: i128 = -1;
    let mut j: i128 = 0;

    const JUMP: f64 = (1u64 << 31) as f64;
    const CONSTANT: u64 = 2862933555777941757; // Values based on the original java implementation

    let mut mut_key = key;

    while j < buckets as i128 {
        b = j;
        mut_key = mut_key.wrapping_mul(CONSTANT).wrapping_add(1);

        let exp = ((mut_key >> 33) + 1).max(1) as f64;
        j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i128;
    }

    Ok(b as u32)
}


 /*
 pub enum OuterCollisionResult {
    Zero,
    Partial(u32),
    Complete(u32, u32),
    Error,
}
  */



pub enum CollisionResult {
    Zero,
    PartialMinor(u32),
    PartialMajor(u32, u32),
    Complete(u32, u32, u32),
    Error,
}

pub fn check_collision(map: &HashMap<u32, bool>) -> Result<CollisionResult> {
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => CollisionResult::Zero,
        [one] => CollisionResult::PartialMinor(**one),
        [one, two] => CollisionResult::PartialMajor(**one, **two),
        [one, two, three] => CollisionResult::Complete(**one, **two, **three),
        _ => CollisionResult::Error,
    };

    Ok(collision_result)
}

fn write_disk_io_cache(data: HashMap<u32, Vec<String>>, cache_type: FilterType) -> Result<()> {
    let node = match cache_type {
        FilterType::Outer => "outer",
        FilterType::Inner => "inner",
    };

    for (shard, keys) in data.into_iter() {
        if let Some(parent) = std::path::Path::new("./pbf_data/init.txt").parent() {
            fs::create_dir_all(parent)?; // Create directory if it does not exist
        }
        let file_name = format!("./pbf_data/{}_{}.txt", node, shard);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_name)?;

        let mut writer = io::BufWriter::new(file);

        for k in keys {
            writer.write_all(k.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;
    }

    Ok(())
}

fn concurrecy_init() -> Result<(usize, usize)> {
    let drain_threads;
    let rehash_threads;

    // Use this function instead of the num_spu crate becasue this can account for virtual machines
    let system_threads = match std::thread::available_parallelism() {
        Ok(c) => c.get(),
        Err(e) => {
            tracing::warn!("Failed to get the core count:{e}");
            return Err(anyhow!("Failed to get the systems thread count"));
        }
    };

    // Base cases
    // If system threads = 2 then 1 drain, 1 rehash,
    if system_threads == 2 { drain_threads = 1; rehash_threads = 1; return Ok((drain_threads, rehash_threads))};
    // if system threads = 4 then 1 drain, 3 rehash
    if system_threads == 4 { drain_threads = 1; rehash_threads = 3; return Ok((drain_threads, rehash_threads))};

    let numerator = system_threads * 8;
    let denominator = 10;
    let rehash_threads = (numerator + denominator - 1) / denominator; // give roughly round(80%) cores to rehash

    drain_threads = system_threads - rehash_threads;
    tracing::info!("System threads: {:?}, rehash threads: {}, drain threads: {}", system_threads, rehash_threads, drain_threads);

    Ok((drain_threads, rehash_threads))
}



#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;

    use once_cell::sync::Lazy;

    use crate::{concurrecy_init, PerfectBloomFilter};
    static COUNT: i32 = 2_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });

    /* #[test]
    fn test_rehash() -> anyhow::Result<()> {
        Lazy::force(&TRACING);

        let _ = concurrecy_init();

        let _ = std::thread::sleep(Duration::from_secs(5));

        Ok(())
    }*/

    



     #[test]
    fn test_filter_loop_sync() -> anyhow::Result<()> {
       
        Lazy::force(&TRACING);

        match std::fs::remove_dir_all("./pbf_data") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }
        match std::fs::remove_dir_all("./metadata") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }
        let mut pf = PerfectBloomFilter::new()?;

        tracing::info!("Starting Insert phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;
            if i % 100_000 == 0 {
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, false);

             
             
        }
        tracing::info!("Completed Insert phase 1");
        //let _ = pf.metadata_dump();
        //let _ = std::thread::sleep(Duration::from_secs(5));

        
        tracing::info!("Starting confirmation phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;
            if i % 100_000 == 0 {
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        let _ = std::thread::sleep(Duration::from_secs(5));
        
         
         
        
         
         
       
        
         
       
         

         

        

        Ok(())
    }
     
   
/*
#[tokio::test]
    async fn test_filter_loop_async() -> anyhow::Result<()> {
        //Lazy::force(&TRACING);

        /*let mut pf = PerfectBloomFilter::new()?;

        tracing::info!("Starting Insert phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, false);
        }
        tracing::info!("Completed Insert phase 1");

        tracing::info!("Starting confirmation phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_insert(&key)?;

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        let _  = tokio::time::sleep(Duration::from_secs(5));
        
         */
        

        Ok(())
       
    }

    

*/



   
}
