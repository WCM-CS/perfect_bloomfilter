use std::collections::HashMap;

use crate::{BloomFilter};
use anyhow::{Result, anyhow};



pub const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const INNER_ARRAY_SHARDS: u32 = 8192;

pub const INNER_BLOOM_STARTING_MULT: u32 = 12;
pub const INNER_BLOOM_STARTING_LENGTH: u64 = 2_u64.pow( INNER_BLOOM_STARTING_MULT);



pub struct InnerBlooms {
    outer_shards: Arc<RwLock<InnerShardArray>>,
    outer_metadata: Arc<RwLock<MetaData>>,
    outer_disk_cache: Arc<Mutex<HashMap<u32, Vec<String>>>>, //cache struct
}


impl Default for InnerBlooms {
    fn default() -> Self {
        Self::new()
    }
}




impl BloomFilter for InnerBlooms {
    fn bloom_hash(
        &self,
        vector_partitions: &[u32],
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.inner_metadata;

        for &vector_slot in vector_partitions {
            let bloom_length_mult = match inner.bloom_bit_length_mult.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let bloom_lenght = raise_to_the_power_of_x(bloom_bit_length_mult);
            

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
    
    type CollisionResult;
    
    fn contains_and_insert(&self, key: &str) -> anyhow::Result<bool> {
        todo!()
    }
    
    fn bloom_check(&self, map: &std::collections::HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> anyhow::Result<crate::CollisionResult> {
        todo!()
    }
    
    fn bloom_insert(&self, outer_hashed: &std::collections::HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str) {
        todo!()
    }
    
    fn array_partition_hash(&self, key: &str) -> anyhow::Result<Vec<u32>> {
        todo!()
    }
}