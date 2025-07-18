use anyhow::{Result, anyhow};
use bitvec::bitvec;
use bitvec::vec::BitVec;
use std::collections::{self, HashMap, HashSet};
use murmur3::murmur3_32;
use std::io::Cursor;

const STATIC_VECTOR_LENGTH: u32 = 4096;
const STATIC_VECTOR_PARTITIONS: usize = 2;

const STATIC_OUTER_BLOOM_SIZE: u32 = 32;
const INITIAL_INNER_BLOOM_SIZE: u32 = 20;

const STATIC_OUTER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(STATIC_OUTER_BLOOM_SIZE);
const STATIC_INNER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(INITIAL_INNER_BLOOM_SIZE);

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 13;

const MIN_INNER_BITS_PER_KEY: f32 = 19.2;

pub struct PerfectBloomFilter {
    big_bloom: BigBloom,
    partitioned_blooms: Dissolver,
    metadata: MetaDataBlooms,
}

impl Default for PerfectBloomFilter {
    fn default() -> Self {
        Self { 
            big_bloom: Default::default(), 
            partitioned_blooms: Default::default(), 
            metadata: Default::default() 
        }
    }
}

impl PerfectBloomFilter {
    pub fn contains_and_insert(&mut self, key: &str) -> Result<bool> {
        // Phase 1
        let key_partitions = self.big_bloom_hash(&key)?;
        let key_exists: bool = self.big_bloom_check(&key_partitions)?;

        if !key_exists { 
            self.big_bloom_insert(&key_partitions);
        }
        println!("BIG bloom: {}", key_exists);

        // Phase 2
        let vector_partitions = self.vector_partition_hash(&key)?;
        let inner_bloom_hashes = self.inner_bloom_hash(&vector_partitions, &key)?;
        let collision_map = self.inner_bloom_check(&inner_bloom_hashes)?;
        let collision_result = check_collision(&collision_map)?;

        // Phase 3
        let collision_result = match collision_result {
            CollisionResult::Zero => {
                self.inner_bloom_insert(&inner_bloom_hashes);
                println!("ENUM: Zero");
                false
            }
            CollisionResult::Partial(partition) => {
                self.inner_bloom_insert(&inner_bloom_hashes);
                println!("ENUM: Partial");
                false
            }
            CollisionResult::Complete(partition_a,partition_b) => { 
                println!("ENUM: Complete");
                true
            },
            CollisionResult::Error => {
                //self.inner_bloom_insert(&inner_bloom_hashes);
                println!("ENUM: Error");
                false
            }        
        };

        println!("Inner bloom: {}", collision_result);

        Ok(key_exists && collision_result)
    }

    

    fn big_bloom_hash(&self, key: &str) -> Result<Vec<u64>> {
        let mut hash_index_list: Vec<u64> =
            Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);

        for seed in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
            let hash_result = murmur3_32(&mut Cursor::new(&key.as_bytes()), seed).unwrap();
            let final_hash = hash_result as u64 % STATIC_OUTER_BLOOM_FULL_LENGTH;
            hash_index_list.push(final_hash);
        }

        Ok(hash_index_list)
    }

    fn big_bloom_check(&self, key_partitions: &Vec<u64>) -> Result<bool> {
        let key_exists = key_partitions
            .iter()
            .all(|&key| self.big_bloom.body[key as usize]);
        Ok(key_exists)
    }

    fn big_bloom_insert(&mut self, key_partitions: &Vec<u64>) {
        key_partitions.iter()
            .for_each(|&bloom_index| {
                self.big_bloom.body.set(bloom_index as usize, true)
            }
        );
    }

    fn vector_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        let mut cached_vector_slot: Option<u32> = None;
        let mut partitions = HashSet::with_capacity(STATIC_VECTOR_PARTITIONS);

        for seed in 0..STATIC_VECTOR_PARTITIONS {
            
            let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed as u32)?;
            let final_hash = hash_result % STATIC_VECTOR_LENGTH;

            if cached_vector_slot.is_some_and(|slot| slot == final_hash) {
                let new_hash = murmur3_32(&mut Cursor::new(key.as_bytes()), 33)?;
                let new_hash_mod = new_hash % STATIC_VECTOR_LENGTH;
                partitions.insert(new_hash_mod);
            } else {
                partitions.insert(final_hash);
            }

            cached_vector_slot = Some(final_hash);
        }

        let partition_vec: Vec<u32> = partitions.into_iter().collect();
        
        Ok(partition_vec)
    }

    fn inner_bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();

        for &vector_slot in vector_partitions {
            let bloom_length = match self.metadata.inner_bloom_bit_length.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for vector_slot {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            for seed in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed)?;
                let final_hash = hash_result as u64 % bloom_length;
                key_hashes.push(final_hash)
            }
            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }


    fn inner_bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<HashMap<u32, bool>> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        
        for (index, map) in map.iter() { 
            let key_exists = map.iter().all(|&key|
                self.partitioned_blooms.body[*index as usize][key as usize]
            );
            collision_map.insert(*index, key_exists);
        }

        Ok(collision_map)
    }

    fn inner_bloom_insert(&mut self, inner_hashed: &HashMap<u32, Vec<u64>>) {
        for (vector_index, hashes) in inner_hashed {
            hashes.iter().for_each(|&bloom_index| {
                self.partitioned_blooms.body[*vector_index as usize].set(bloom_index as usize, true)
            
            });
        }
    }

}


fn check_collision(map: &HashMap<u32, bool>) -> Result<CollisionResult>{
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => CollisionResult::Zero,
        [one] => CollisionResult::Partial(**one),
        [one, two] => CollisionResult::Complete(**one, **two),
        _ => CollisionResult::Error
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

    big_bloom_bit_length: u64, 
    inner_bloom_bit_length: HashMap<u32, u64>, 
}

impl Default for MetaDataBlooms {
    fn default() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);

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
    Partial(u32),
    Complete(u32, u32),
    Error
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_filter_loop() {
        let count = 1_000_000;
        let mut pf = PerfectBloomFilter::default();
        for i in 1..count {
            let key = i.to_string();
            let result_insert = pf.contains_and_insert(&key).unwrap();
            assert_eq!(result_insert, false);
        }

        for i in 1..count {
            let key = i.to_string();
            let was_present = pf.contains_and_insert(&key).unwrap();
        if !was_present {
            println!("BUG: Key '{}' was missing after supposed insertion!", key);
            // Optionally: panic! or break here to see the first failure
        }
            assert_eq!(was_present, true);
        }
 
        
    }
}