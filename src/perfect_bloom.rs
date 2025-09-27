
use std::io::{BufRead, BufReader, Write};
use std::{fs, io};
use std::sync::RwLock; // Locking
use anyhow::{Result, anyhow}; // Error handling

use bitvec::vec::BitVec; // Bitvec crate
use bitvec::bitvec; // Bitvec Macro

pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x8badf00d,
    0xdeadbabe,
    0xabad1dea,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];

pub struct PerfectBloomFilter {
    pub(crate) cartographer: Vec<RwLock<Shard>>,
    pub(crate) inheritor: Vec<RwLock<Shard>>,

}

impl PerfectBloomFilter {
    pub fn new(shard_vector_len_mult: u32, filter_len_mult: u32, filter_hash_family_size: u32) -> Result<Self> {
        if let Some(parent) = std::path::Path::new("./data/pbf_data/init.txt").parent() {
            fs::create_dir_all(parent)?; // Create directory if it does not exist
        }

        let shard_vector_len = 1usize << shard_vector_len_mult;

        let mut cartographer = Vec::with_capacity(shard_vector_len as usize);
        let mut inheritor = Vec::with_capacity(shard_vector_len as usize);

        for shard_id in 0..shard_vector_len {
            cartographer.push(RwLock::new(Shard::new_cartographer_shard(
                0,
                shard_id as u32,
                1u64 << filter_len_mult,
                filter_len_mult,
                filter_hash_family_size,
            )?));

            inheritor.push(RwLock::new(Shard::new_inheritor_shard(
                0,
                shard_id as u32,
                1u64 << filter_len_mult,
                filter_len_mult,
                filter_hash_family_size,
            )?));
        }

        Ok(Self {
            cartographer,
            inheritor,
        })
    }

    pub fn contains(&self, key: &str) -> Result<bool> {
        let cart_exists = Self::existence_check(&self, key, &ShardType::Cartographer)?;
        let inher_exists = Self::existence_check(&self, key, &ShardType::Inheritor)?;

       Ok(cart_exists & inher_exists)
    }

    pub fn insert(&self, key: &str) -> Result<()> {
        Self::insert_key(&self, key, &ShardType::Cartographer)?;
        Self::insert_key(&self, key, &ShardType::Inheritor)?;

        Ok(())
    }

    fn existence_check(&self, key: &str, shard_type: &ShardType) -> Result<bool> {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type)?;

        for shard in shards {
            let shard = &shard_vec[shard].read().unwrap();
            let hashes = shard.bloom_hash(key)?;
            let exist = shard.bloom_check(&hashes)?;

            if !exist {
                return Ok(false);
            }
        };

        Ok(true)
    }

    fn insert_key(&self, key: &str, shard_type: &ShardType) -> Result<bool> {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type)?;

        for shard in shards {
            let shard = &mut shard_vec[shard].write().unwrap();
            let hashes = shard.bloom_hash(key)?;
            shard.bloom_insert(&hashes, key)?;
            let rehash = shard.rehash_check()?;
            shard.drain(rehash)?;

            // rehash process
            if rehash{
                shard.rehash_bloom()?;
            }
        };

        Ok(true)
    }





    fn array_sharding_hash(&self, key: &str, shard_type: &ShardType) -> Result<Vec<usize>> {
        let (hash_seed, mask) = match shard_type {
            ShardType::Cartographer => (HASH_SEED_SELECTION[0], self.cartographer.len() - 1),
            ShardType::Inheritor => (HASH_SEED_SELECTION[1], self.inheritor.len() - 1),
        };

        let key_slice: &[u8] = key.as_bytes();
        let hash = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seed as u64);

        let high = (hash >> 64) as u64;
        let low = hash as u64;

        let shard_size = mask / 2;
        let xor = high^low;
        let bloom_len = mask + 1;
        let p1 = jump_hash_partition(xor, bloom_len)?;
        let p2 = (p1 + shard_size) & mask;

        debug_assert!(
            p1 != p2,
            "Partitions must be unique"
        );

        Ok(vec![p1, p2])
    }


}





pub struct Shard {
    // Bloomfilter - immutable
    filter: BitVec,
    filter_layer: ShardType,
    shard_id: u32,

    // shard full id lmdb key range: 'filter_layer:shard_id:0..key_count'

    // Metadata - mutable
    key_count: u64,
    bloom_length: u64,
    bloom_length_mult: u32,
    hash_family_size: u32,

    // Cache
    key_cache: Vec<String>,
    file_path: String,
}


impl Shard {
    pub fn bloom_hash(
        &self,
        key: &str,
    ) -> Result<Vec<u64>> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };
        let key_slice: &[u8] = key.as_bytes();

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[0].into());
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key_slice, hash_seeds[1].into());

        let hash_family_size = &self.hash_family_size;
        let bloom_length = &self.bloom_length;

        let mut hash_list = Vec::with_capacity(*hash_family_size as usize);
        for idx in 0..*hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (bloom_length - 1) as u128;
            // Kirsch-Mitzenmacher optimization
            let index  = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

            hash_list.push(index as u64)
        }

        Ok(hash_list)
    }

    fn bloom_insert(&mut self, hashes: &Vec<u64>, key: &str) -> Result<()> {
        hashes.iter().for_each(|hash| self.filter.set(*hash as usize, true));
        self.key_count += 1;
        self.key_cache.push(key.to_string());

        Ok(())
    }

    fn bloom_check(&self, hashes: &Vec<u64>) -> Result<bool> {
        let res = hashes.iter().all(|hash| self.filter[*hash as usize]);
        Ok(res)
    }

    fn drain(&mut self, force_drain: bool) -> Result<()>{
        if self.key_cache.len() >= 50 || force_drain {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file_path)?;

            let mut writer = io::BufWriter::new(file);
            let keys = std::mem::take(&mut self.key_cache);
            for k in keys {
                writeln!(writer, "{}", k)?; // Combine key + newline
            }

            writer.flush()?;
        }

        Ok(())

    }

    fn rehash_check(&self) -> Result<bool> {
        // 19.2 rehsah constant to start
        if (self.bloom_length as f64 / self.key_count as f64) <= 19.2 {
            Ok(true)
        } else {
            Ok(false)
        }
    }


    fn rehash_bloom(&mut self) -> Result<()> {
        let file = fs::File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        let new_bloom_length = 1usize << (self.bloom_length_mult + 1);
        let new_bloomfilter = bitvec![0; new_bloom_length];

        self.filter = new_bloomfilter;
        self.bloom_length = new_bloom_length as u64;
        self.bloom_length_mult = self.bloom_length_mult + 1;



        for line in reader.lines() {
            let key = line?.trim_end().to_string();
            let hashes = self.bloom_hash(&key)?;
            for &hash in &hashes {
                self.filter.set(hash as usize, true);
            }
        }


        Ok(())
    }









    fn new_cartographer_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: u64,
        filter_starting_len_mult: u32,
        filter_starting_hash_family: u32
    ) -> Result<Self> {

        Ok(Self {
            filter: bitvec![0; filter_starting_len as usize],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Cartographer,
            shard_id: shard_id,
            key_cache: vec![],
            file_path: format!("./data/pbf_data/{}_{}.txt", ShardType::Cartographer.as_static_str(), shard_id)

        })
    }

    fn new_inheritor_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: u64,
        filter_starting_len_mult: u32,
        filter_starting_hash_family: u32
    ) -> Result<Self> {

        Ok(Self {
            filter: bitvec![0; filter_starting_len as usize],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Inheritor,
            shard_id: shard_id,
            key_cache: vec![],
            file_path: format!("./data/pbf_data/{}_{}.txt", ShardType::Inheritor.as_static_str(), shard_id)

        })
    }
    /*
    fn new_harbinger_shard(
        key_count: u64,
        shard_id: u32,
        filter_starting_len: u64,
        filter_starting_len_mult: u32,
        filter_starting_hash_family: u32
    ) -> Result<Self> {

        Ok(Self {
            filter: bitvec![0; filter_starting_len as usize],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: filter_starting_hash_family,
            filter_layer: ShardType::Harbinger,
            shard_id: shard_id,

            lock: RwLock::new(()),
        })
    }
     */




}



#[derive(Eq, Hash, PartialEq, Clone)]
pub enum ShardType {
    Cartographer,
    Inheritor,
   // Harbinger,
}

impl ShardType {
    pub fn as_static_str(&self) -> &'static str {
        match self {
            ShardType::Cartographer => "cartographer",
            ShardType::Inheritor => "inheritor",
           // ShardType::Harbinger => "harbinger",
        }
    }
}





// JumpConsistentHash function, rust port from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
// Original algorithm founded in 2014 by google Lamping & Veach

const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

pub fn jump_hash_partition(key: u64, buckets: usize) -> Result<usize> {
    if buckets == 0 {
        return Err(anyhow!("Number of buckets must be positive"));
    }

    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut mut_key = key;

    while j < buckets as i64 {
        b = j;
        mut_key = mut_key.wrapping_mul(JUMP_HASH_CONSTANT).wrapping_add(1);

        let shifted = (mut_key >> JUMP_HASH_SHIFT).wrapping_add(1);
        let exp = shifted.max(1) as f64;

        j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i64;
    }

    Ok(b as usize)
}



#[cfg(test)]
mod tests {
    use std::time::Duration;
    use anyhow::Result;

    use once_cell::sync::Lazy;

    use crate::perfect_bloom::PerfectBloomFilter;

    static COUNT: i32 = 1_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });





    #[test]
    fn test_insert_and_contains_function() -> Result<()> {
        Lazy::force(&TRACING);

        match std::fs::remove_dir_all("./data/pbf_data") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }
        match std::fs::remove_dir_all("./data/metadata") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }
       
        tracing::info!("Creating PerfectBloomFilter instance");
        //et config = Config::
        let shard_vector_len_mult = 12;
        let shard_filter_len_milt = 12;
        let hash_family_size = 7;
        let pf = PerfectBloomFilter::new(shard_vector_len_mult, shard_filter_len_milt, hash_family_size)?;

        tracing::info!("PerfectBloomFilter created successfully");


        tracing::info!("Contains & insert & contains check");
        for i in 0..COUNT {
            let key = i.to_string();

            let was_present_a = pf.contains(&key)?;


            if was_present_a {
                tracing::warn!("Fasle positive: {}", key);
            }



            assert_eq!(was_present_a, false);

            let _ = pf.insert(&key)?;


            let was_present_b = pf.contains(&key)?;

            if !was_present_b {
                tracing::warn!("Fasle negative: {}", key);
            }


            assert_eq!(was_present_b, true);
            

        }

     


        //dump_metadata();

        std::thread::sleep(Duration::from_millis(100));

        Ok(())
    }

}