use anyhow::{anyhow, Result};
use bitvec::bitvec;
use bitvec::vec::BitVec;
use murmur3::murmur3_32;
use std::collections::{HashMap};
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const STATIC_VECTOR_LENGTH: u32 = 4096;
const STATIC_VECTOR_PARTITIONS: usize = 2;

const STATIC_OUTER_BLOOM_SIZE: u32 = 32;
const INITIAL_INNER_BLOOM_SIZE: u32 = 20;

const STATIC_OUTER_BLOOM_FULL_LENGTH: u128 = 2_u128.pow(STATIC_OUTER_BLOOM_SIZE);
const STATIC_INNER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(INITIAL_INNER_BLOOM_SIZE);

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 13;

const MIN_OUTER_BITS_PER_KEY: f32 = 9.2;
const MIN_INNER_BITS_PER_KEY: f32 = 19.2;

pub struct PerfectBloomFilter {
    big_bloom: BigBloom,
    partitioned_blooms: Dissolver,
    metadata: MetaDataBlooms,
    disk_io_cache: Vec<(u32, String)>,
}

impl Default for PerfectBloomFilter {
    fn default() -> Self {
        Self {
            big_bloom: Default::default(),
            partitioned_blooms: Default::default(),
            metadata: Default::default(),
            disk_io_cache: Vec::new(),
        }
    }
}

impl PerfectBloomFilter {
    fn new_pbf_with_thread() -> Arc<Mutex<PerfectBloomFilter>> {
        let filter = Arc::new(Mutex::new(Self::default()));
        let filter_clone = Arc::clone(&filter);

        // DISK IO THREAD
        thread::spawn(move || {
            // spawn a thread for handling disk io, writing to disk from cache
            loop {
                {
                    let mut pbf = filter_clone.lock().unwrap();
                    // do the meta data calculations here
                }
                thread::sleep(Duration::from_secs(60));
            }
        });

        thread::spawn(move || {
            // spawn a thread for handling the resizing of the inner bloomfilters
            loop {
                {
                    // let mut pbf = filter_clone.lock().unwrap();
                    // do the meta data calculations here
                }
                thread::sleep(Duration::from_secs(60));
            }
        });

        filter
    }

    pub fn contains_and_insert(&mut self, key: &str) -> Result<bool> {
        // Phase 1
        let outer_bloom_key_idx = self.big_bloom_hash(key)?;
        let outer_bloom_key_exists: bool = self.big_bloom_check(&outer_bloom_key_idx)?;

        if !outer_bloom_key_exists {
            self.big_bloom_insert(&outer_bloom_key_idx);
        }

        
        // Phase 2
        let vector_idx = self.vector_partition_hash(key)?;
        let inner_bloom_key_idx = self.inner_bloom_hash(&vector_idx, key)?;
        let collision_map = self.inner_bloom_check(&inner_bloom_key_idx)?;
        let collision_result: CollisionResult = check_collision(&collision_map)?;


        let inner_bloom_key_exists = matches!(collision_result, CollisionResult::Complete(_, _));
        
        match collision_result {
            CollisionResult::Zero | CollisionResult::Partial(_) => {
                self.inner_bloom_insert(&inner_bloom_key_idx);
            }
            CollisionResult::Error => {
                panic!("unexpected issue with collision result for key: {key}");
            }
            _ => {}
        }

        /*
           let is_reported_present = outer_bloom_key_exists && inner_bloom_key_exists;

        // Print diagnostic info if filter thinks it already saw this key
        if is_reported_present {
            println!("FALSE POSITIVE POSSIBLE:");
            println!("  Key: {}", key);
            println!("  Outer bloom: PRESENT");
            println!("  Inner bloom result: {:?}", collision_result);
            println!("  Outer indices: {:?}", outer_bloom_key_idx);
            println!("  Inner partitions: {:?}", vector_idx);
            println!("  Inner indices map: {:?}", inner_bloom_key_idx);
        }
         */

        
     

         

    

        // Phase 4
        // On deck
        //self.insert_disk_io_cache(key, vector_idx)?;

        Ok(outer_bloom_key_exists && matches!(collision_result, CollisionResult::Complete(_, _)))
    }

    fn insert_disk_io_cache(&mut self, key: &str, vector_idx: Vec<u32>) -> Result<()> {
        for idx in vector_idx {
            self.disk_io_cache.push((idx, key.to_owned()));
        }

        Ok(())
    }

    fn big_bloom_hash(&self, key: &str) -> Result<Vec<u64>> {
        let mut hash_index_list: Vec<u64> =
            Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);
        let hash_const = 8;

        let h1 = murmur3_32(&mut Cursor::new(key.as_bytes()), hash_const)?;
        let h2 = murmur3_32(&mut Cursor::new(key.as_bytes()), hash_const.wrapping_add(55))?;

        for idx in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
            let index = (h1.wrapping_add(idx.wrapping_mul(h2))) as u64 % STATIC_OUTER_BLOOM_FULL_LENGTH as u64;

            hash_index_list.push(index.into());
        }

        Ok(hash_index_list)
    }

    fn big_bloom_check(&self, key_partitions: &[u64]) -> Result<bool> {
        let key_exists = key_partitions
            .iter()
            .all(|&key| self.big_bloom.body[key as usize]);
        Ok(key_exists)
    }

    fn big_bloom_insert(&mut self, key_partitions: &[u64]) {
        key_partitions
            .iter()
            .for_each(|&bloom_index| self.big_bloom.body.set(bloom_index as usize, true));

        self.metadata.big_bloom_key_count += 1;
    }

    fn vector_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        let h1 = murmur3_32(&mut Cursor::new(key.as_bytes()), 33)? % STATIC_VECTOR_LENGTH;

        let p1 = h1;
        let p2 = (h1 + STATIC_VECTOR_LENGTH / 2) % STATIC_VECTOR_LENGTH;

        Ok(vec![p1, p2])
    }

    fn inner_bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str,
    ) -> Result<HashMap<u32, Vec<u32>>> {
        let mut inner_hash_list = HashMap::new();

        for &vector_slot in vector_partitions {
            let bloom_length = match self.metadata.inner_bloom_bit_length.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for vector_slot {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 = murmur3_32(&mut Cursor::new(key.as_bytes()), vector_slot)?;
            let h2 = murmur3_32(&mut Cursor::new(key.as_bytes()), vector_slot.wrapping_add(55))?;
            
            for i in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let index = (h1.wrapping_add(i.wrapping_mul(h2))) % bloom_length as u32;

                key_hashes.push(index)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn inner_bloom_check(&self, map: &HashMap<u32, Vec<u32>>) -> Result<HashMap<u32, bool>> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| self.partitioned_blooms.body[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        Ok(collision_map)
    }

    fn inner_bloom_insert(&mut self, inner_hashed: &HashMap<u32, Vec<u32>>) {
        for (vector_index, hashes) in inner_hashed {
            hashes.iter().for_each(|&bloom_index| {
                self.partitioned_blooms.body[*vector_index as usize].set(bloom_index as usize, true)
            });
            
            self.metadata
                .inner_blooms_key_count
                .entry(*vector_index)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }
}

fn check_collision(map: &HashMap<u32, bool>) -> Result<CollisionResult> {
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => CollisionResult::Zero,
        [&one] => CollisionResult::Partial(one),
        [&one, &two] => CollisionResult::Complete(one, two),
        _ => CollisionResult::Error,
    };

    Ok(collision_result)
}

struct BigBloom {
    body: BitVec,
}

impl Default for BigBloom {
    fn default() -> Self {
        Self {
            body: bitvec![0; STATIC_OUTER_BLOOM_FULL_LENGTH as usize],
        }
    }
}

#[derive(Debug)]
struct Dissolver {
    body: Vec<BitVec>,
}

impl Default for Dissolver {
    fn default() -> Self {
        let mut body = Vec::with_capacity(STATIC_VECTOR_LENGTH as usize);
        for _ in 0..STATIC_VECTOR_LENGTH {
            body.push(bitvec![0; STATIC_INNER_BLOOM_FULL_LENGTH as usize]);
        }
        Self { body }
    }
}

struct MetaDataBlooms {
    big_bloom_key_count: u64,
    inner_blooms_key_count: HashMap<u32, u64>,

    big_bloom_bit_length: u128,
    inner_bloom_bit_length: HashMap<u32, u64>,
}

impl Default for MetaDataBlooms {
    fn default() -> Self {
        let mut key_count: HashMap<u32, u64> =
            HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);
        let mut bit_length: HashMap<u32, u64> =
            HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);

        for partition in 0..STATIC_VECTOR_LENGTH {
            key_count.insert(partition, 0);
            bit_length.insert(partition, STATIC_INNER_BLOOM_FULL_LENGTH);
        }

        Self {
            big_bloom_key_count: 0,
            inner_blooms_key_count: key_count,

            big_bloom_bit_length: STATIC_OUTER_BLOOM_FULL_LENGTH,
            inner_bloom_bit_length: bit_length,
        }
    }
}

#[derive(Debug)]
enum CollisionResult {
    Zero,
    Partial(u32),
    Complete(u32, u32),
    Error,
}

#[cfg(test)]
mod tests {
    use std::time::Instant;
    use crate::PerfectBloomFilter;

    #[test]
    fn test_filter() {
        let temp_key1 = "First_Value".to_string(); // false
        let temp_key2 = "First_Value".to_string(); // true
        let temp_key3 = "First_Value_new".to_string(); // false
        let temp_key4 = "First_Value".to_string(); // true
        let temp_key5 = "First_Value_new".to_string(); // true
        let temp_key6 = "First_Value_newa".to_string(); // false

        let mut pf = PerfectBloomFilter::default();
       
        let result_a = pf.contains_and_insert(&temp_key1).unwrap();
        let result_b = pf.contains_and_insert(&temp_key2).unwrap();
        let result_c = pf.contains_and_insert(&temp_key3).unwrap();
        let result_d = pf.contains_and_insert(&temp_key4).unwrap();
        let result_e = pf.contains_and_insert(&temp_key5).unwrap();
        let result_f = pf.contains_and_insert(&temp_key6).unwrap();

        assert_eq!(result_a, false);
        assert_eq!(result_b, true);
        assert_eq!(result_c, false);
        assert_eq!(result_d, true);
        assert_eq!(result_e, true);
        assert_eq!(result_f, false);
    }

    #[tokio::test]
    async fn test_filter_loop() {
        let count = 5_000_000;
        let pf = PerfectBloomFilter::new_pbf_with_thread();

        println!("Phase 1: Inserting {} new keys..", count);
        let start = Instant::now();

        for i in 0..count {
            let key = i.to_string();
            let mut pf_guard = pf.lock().expect("Failed to lock bloom filter");
            let is_present = pf_guard.contains_and_insert(&key).expect("Insert failed unexpectedly");

            assert_eq!(is_present, false);
        }

        let dur1 = start.elapsed();
        eprintln!("Phase done in {:.2?}", dur1);

       
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf.lock().unwrap().contains_and_insert(&key).unwrap();
            if !was_present {
                println!("BUG: Key '{}' was missing after supposed insertion!", key);
            }
            assert_eq!(was_present, true);
        }

        let dur2 = start.elapsed();
        eprintln!("Phase done in {:.2?}", dur2);
        
        let count2 = 10_000_000;
        for i in count..count2 {
            let key = i.to_string();
            let was_present = pf.lock().unwrap().contains_and_insert(&key).unwrap();
            if !was_present {
                println!("BUG: Key '{}' was missing after supposed insertion!", key);
            }
            assert_eq!(was_present, false);
        }

        let dur3 = start.elapsed();
        eprintln!("Phase done in {:.2?}", dur3);

        for i in count..count2 {
            let key = i.to_string();
            let was_present = pf.lock().unwrap().contains_and_insert(&key).unwrap();
            if !was_present {
                println!("BUG: Key '{}' was missing after supposed insertion!", key);
            }
            assert_eq!(was_present, true);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;


        let dur4 = start.elapsed();
        eprintln!("Phase done in {:.2?}", dur4);
        
        
    }
}
