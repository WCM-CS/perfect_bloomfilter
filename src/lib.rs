use anyhow::{anyhow, Result};
use bitvec::bitvec;
use bitvec::vec::BitVec;
use murmur3;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

type VectorPartition = u32;
type Key = String;

const STATIC_VECTOR_LENGTH: u32 = 4096;
const STATIC_VECTOR_PARTITIONS: u32 = 2;

const STATIC_OUTER_BLOOM_SIZE: u32 = 32;
const INITIAL_INNER_BLOOM_SIZE: u32 = 20;

const STATIC_OUTER_BLOOM_FULL_LENGTH: u128 = 2_u128.pow(STATIC_OUTER_BLOOM_SIZE);
const STATIC_INNER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(INITIAL_INNER_BLOOM_SIZE);

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 13;

const MIN_OUTER_BITS_PER_KEY: f32 = 9.2;
const MIN_INNER_BITS_PER_KEY: f32 = 19.2;

static HASH_SEED_SELECTION: [u32; 5] = [9, 223, 372, 630, 854];

#[derive(Default)]
pub struct PerfectBloomFilter {
    big_bloom: BigBloom,
    partitioned_blooms: Dissolver,
    metadata: MetaDataBlooms,
    disk_io_cache: Vec<(VectorPartition, Key)>,
}

impl PerfectBloomFilter {
    fn new_pbf_with_thread() -> Arc<Mutex<PerfectBloomFilter>> {
        let filter = Arc::new(Mutex::new(Self::default()));
        let filter_clone = Arc::clone(&filter);

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

        // DISK IO THREAD

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

    // Using double hashing of two murmur3_u128 hashes
    fn big_bloom_hash(&self, key: &str) -> Result<Vec<u64>> {
        let mut hash_index_list: Vec<u64> =
            Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);

        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[0])?;
        let h2 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[1])?;

        for idx in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
            let idx_u128 = idx as u128;
            let index = h1.wrapping_add(idx_u128.wrapping_mul(h2)) % STATIC_OUTER_BLOOM_FULL_LENGTH;

            hash_index_list.push(index as u64);
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
        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;

        let p1 = self.jump_hash_partition(high, STATIC_VECTOR_LENGTH)?;
        let mut p2 = self.jump_hash_partition(low, STATIC_VECTOR_LENGTH)?;

        // utilize deterministic rehashing with xor principle
        if p1 == p2 {
            let rehashed = self.jump_hash_partition(high ^ low, STATIC_VECTOR_LENGTH)?;

            p2 = if rehashed != p1 {
                rehashed
            } else {
                (p1 + ((low ^ high) % (STATIC_VECTOR_LENGTH as u64 - 1) + 1) as u32)
                    % STATIC_VECTOR_LENGTH
            }
        }

        Ok(vec![p1 as u32, p2 as u32])
    }

    // JumpConsistentHash function, rewritten from the jave implementation
    // Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
    fn jump_hash_partition(&self, key: u64, buckets: u32) -> Result<u32> {
        // Values based on the original java implementation
        let mut b: i128 = -1;
        let mut j: i128 = 0;

        const JUMP: f64 = (1u64 << 31) as f64;
        const CONSTANT: u64 = 2862933555777941757;

        let mut mut_key = key;

        while j < buckets as i128 {
            b = j;
            mut_key = mut_key.wrapping_mul(CONSTANT).wrapping_add(1);

            let exp = ((mut_key >> 33) + 1).max(1) as f64;
            j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i128;
        }

        Ok(b as u32)
    }

    fn inner_bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();

        for &vector_slot in vector_partitions {
            let bloom_length = match self.metadata.inner_bloom_bit_length.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for vector_slot {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[4])?;

            for idx in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128;
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    fn inner_bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<HashMap<u32, bool>> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| self.partitioned_blooms.body[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        Ok(collision_map)
    }

    fn inner_bloom_insert(&mut self, inner_hashed: &HashMap<u32, Vec<u64>>) {
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

    fn dump_inner_bloom_data(&self) {
        self.metadata
            .inner_blooms_key_count
            .iter()
            .for_each(|(key, value)| {
                tracing::info!("Vector partition: {key}\nKey insertions: {value}\n");
            })
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
        [&one] => CollisionResult::Partial(()),
        [&one, &two] => CollisionResult::Complete((), ()),
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
    inner_blooms_key_count: HashMap<VectorPartition, u64>,

    big_bloom_bit_length: u128,
    inner_bloom_bit_length: HashMap<VectorPartition, u64>,
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

enum CollisionResult {
    Zero,
    Partial(()),
    Complete((), ()),
    Error,
}

#[cfg(test)]
mod tests {
    use crate::PerfectBloomFilter;
    use std::time::Instant;

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

    /*
     */
    #[tokio::test]
    async fn test_filter_loop() {
        tracing_subscriber::fmt::init();
        let count = 100_000_000;
        let pf = PerfectBloomFilter::new_pbf_with_thread();

        let mut locked_pf = pf.lock().unwrap();

        tracing::info!("Starting Insert phase 1");
        for i in 0..count {
            let key = i.to_string();
            let is_present = locked_pf
                .contains_and_insert(&key)
                .expect("Insert failed unexpectedly");

            assert_eq!(is_present, false);
        }
        tracing::info!("Completed Insert phase 1");

        tracing::info!("Starting confirmation phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = locked_pf
                .contains_and_insert(&key)
                .expect("Insert failed unexpectedly");

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        tracing::info!("Starting insertion phase 2");
        // worked at value of 60 million keys for no collisoin as is
        let count2 = 200_000_000;
        for i in count..count2 {
            let key = i.to_string();
            let was_present = locked_pf
                .contains_and_insert(&key)
                .expect("Insert failed unexpectedly");

            if was_present {
                tracing::error!("Value was a false positive: {key}, Jikes buster, you suck");
                locked_pf.dump_inner_bloom_data();
            }

            assert_eq!(was_present, false);
        }
        tracing::info!("Completed insertion phase 2");

        tracing::info!("Starting confirmation phase 2");
        for i in count..count2 {
            let key = i.to_string();
            let was_present = locked_pf.contains_and_insert(&key).unwrap();
            if !was_present {
                println!("BUG: Key '{}' was missing after supposed insertion!", key);
            }
            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 2");
    }
}
