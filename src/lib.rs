use bitvec::bitvec;
use bitvec::vec::BitVec;
use once_cell::sync::OnceCell;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::RwLock;
use std::hash::Hash;
use tempfile::TempDir;

static REHASH_THRESHOLD: OnceCell<f64> = OnceCell::new();
static REHASH_SWITCH: OnceCell<bool> = OnceCell::new();

pub struct PerfectBloomFilter {
    pub(crate) cartographer: Vec<RwLock<Shard>>,
    pub(crate) inheritor: Vec<RwLock<Shard>>,
    #[allow(dead_code)]
    temp_dir: TempDir
}

impl PerfectBloomFilter {
    pub fn new() -> Self {
        let config = BloomFilterConfig::default();
        let (cartographer, inheritor, temp_dir) = Self::pbf_init(config);

        Self {
            cartographer,
            inheritor,
            temp_dir
        }
    }

    pub fn new_with_config(config: BloomFilterConfig) -> Self {
        let (cartographer, inheritor, temp_dir) = Self::pbf_init(config);

        Self {
            cartographer,
            inheritor,
            temp_dir
        }
    } 
    
    pub fn contains(&self, key: &[u8]) -> bool {
        let cart_exists = Self::existence_check(&self, key, &ShardType::Cartographer);
        let inher_exists = Self::existence_check(&self, key, &ShardType::Inheritor);

        cart_exists & inher_exists
    }

    pub fn insert(&self, key: &[u8]) {
        Self::insert_key(&self, &key, &ShardType::Cartographer);
        Self::insert_key(&self, &key, &ShardType::Inheritor);
    }

    fn existence_check(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = &shard_vec[shard].read().unwrap();
            let hashes = shard.bloom_hash(key);
            let exist = shard.bloom_check(&hashes);

            if !exist {
                return false;
            }
        }

        true
    }

    fn insert_key(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = &mut shard_vec[shard].write().unwrap();
            let hashes = shard.bloom_hash(key);
            shard.bloom_insert(&hashes, key);
            let rehash = shard.rehash_check();
            shard.drain(rehash);

            rehash.then(|| {
                if *REHASH_SWITCH.get().unwrap() {
                    shard.rehash_bloom()
                }
            });
        }

        true
    }

    fn array_sharding_hash(&self, key: &[u8], shard_type: &ShardType) -> Vec<usize> {
        let (hash_seed, mask) = match shard_type {
            ShardType::Cartographer => (HASH_SEED_SELECTION[0], self.cartographer.len() - 1),
            ShardType::Inheritor => (HASH_SEED_SELECTION[1], self.inheritor.len() - 1),
        };

        let hash = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seed);

        let high = (hash >> 64) as u64;
        let low = hash as u64;

        let p1 = jump_hash_partition(high ^ low, mask + 1);
        let p2 = (p1 + (mask/2)) & mask;

        debug_assert!(p1 != p2, "Partitions must be unique");

        vec![p1, p2]
    }

    fn pbf_init(config: BloomFilterConfig) -> (Vec<RwLock<Shard>>, Vec<RwLock<Shard>>, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let shard_vector_len_mult = match config.throughput() {
            Throughput::Low => 11, // 2048
            Throughput::Medium => 12, // 4096
            Throughput::High => 13, // 8192
        };

        let filter_len_mult = match config.initial_capacity() {
            Capacity::Low => 11,
            Capacity::Medium => 12,
            Capacity::High => 13,
            Capacity::VeryHigh => 15
        };
        
        let threshold = match config.accuracy() {
            Accuracy::Low => 12.0, 
            Accuracy::Medium => 15.0,
            Accuracy::High => 19.0,
        };
         
        REHASH_THRESHOLD.set(threshold).ok();
        REHASH_SWITCH.set(config.rehash()).ok();

        let shard_vector_len = 1usize << shard_vector_len_mult;

        let mut cartographer = Vec::with_capacity(shard_vector_len);
        let mut inheritor = Vec::with_capacity(shard_vector_len);

        for shard_id in 0..shard_vector_len {
            let carto_path = base_path.join(format!("{}_{}.tmp", ShardType::Cartographer.as_static_str(), shard_id));
            let inheritor_path= base_path.join(format!("{}_{}.tmp", ShardType::Inheritor.as_static_str(), shard_id));

            cartographer.push(RwLock::new(Shard::new_cartographer_shard(
                0,
                1usize << filter_len_mult,
                filter_len_mult,
                carto_path,
            )));

            inheritor.push(RwLock::new(Shard::new_inheritor_shard(
                0,
                1usize << filter_len_mult,
                filter_len_mult,
                inheritor_path,
            )));
        }
        
        (cartographer, inheritor, temp_dir)
    }
}

pub struct Shard {
    filter: BitVec,
    filter_layer: ShardType,
    key_count: u64,
    bloom_length: usize,
    bloom_length_mult: usize,
    hash_family_size: usize,
    key_cache: Vec<Vec<u8>>,
    storage_path: PathBuf,
}

impl Shard {
    pub fn bloom_hash(&self, key: &[u8]) -> Vec<usize> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);

        let mut hash_list = Vec::with_capacity(*&self.hash_family_size);
        for idx in 0..*&self.hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (&self.bloom_length - 1) as u128;

            // Kirsch-Mitzenmacher optimization
            let index = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask; 

            hash_list.push(index as usize)
        }

        hash_list
    }

    fn bloom_insert(&mut self, hashes: &Vec<usize>, key: &[u8]) {
        hashes.iter().for_each(|hash| self.filter.set(*hash, true));
        self.key_count += 1;
        self.key_cache.push(key.to_vec());
    }

    fn bloom_check(&self, hashes: &Vec<usize>) -> bool {
        let res = hashes.iter().all(|hash| self.filter[*hash]);
        res
    }

    fn drain(&mut self, force_drain: bool) {
        if self.key_cache.len() >= 50 || force_drain {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.storage_path)
                .unwrap();

            let mut writer = BufWriter::new(file);
            let keys = std::mem::take(&mut self.key_cache);
          
            for bytes in keys {
                let len = bytes.len() as u32;
                writer.write_all(&len.to_be_bytes()).ok();
                writer.write_all(&bytes).ok();
            }
            
            writer.flush().ok();
        }
    }

    fn rehash_check(&self) -> bool {
        if (self.bloom_length as f64 / self.key_count as f64) <= *REHASH_THRESHOLD.get().unwrap() {
            true
        } else {
            false
        }
    }

    fn expected_n(new_m: usize, bits_per_key_threshold: f64) -> usize {
        (new_m as f64 / bits_per_key_threshold).floor() as usize
    }

    fn optimal_k(m: usize, n: usize) -> usize {
        ((m as f64 / n as f64) * std::f64::consts::LN_2).round() as usize
    }

    fn rehash_bloom(&mut self) {
        let file = fs::File::open(&self.storage_path).unwrap();
        let mut reader = BufReader::new(file);

        let new_bloom_length = 1usize << (self.bloom_length_mult + 1);
        let mut new_bloomfilter = bitvec![0; new_bloom_length];
        let expected_n = Self::expected_n(new_bloom_length, *REHASH_THRESHOLD.get().unwrap());
        let new_k = Self::optimal_k(new_bloom_length, expected_n);

        self.bloom_length = new_bloom_length;
        self.bloom_length_mult += 1;
        self.hash_family_size = new_k;

        let mut len_bytes = [0u8; 4];
        let mut key_buf = Vec::with_capacity(1024); 

        while reader.read_exact(&mut len_bytes).is_ok() {
            let len = u32::from_be_bytes(len_bytes) as usize;
            if key_buf.len() < len {
                key_buf.resize(len, 0);
            }
      
            reader.read_exact(&mut key_buf[..len]).unwrap();
            let hashes = self.bloom_hash(&key_buf[..len]);
            for hash in hashes {
                new_bloomfilter.set(hash, true);
            }
        }

        self.filter = new_bloomfilter;
    }

    fn new_cartographer_shard(
        key_count: u64,
        filter_starting_len: usize,
        filter_starting_len_mult: usize,
        storage_path: PathBuf,
    ) -> Self {
        let n = Self::expected_n(filter_starting_len, *REHASH_THRESHOLD.get().unwrap());
        let hash_family_size = Self::optimal_k(filter_starting_len, n);

        Self {
            filter: bitvec![0; filter_starting_len],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: hash_family_size,
            filter_layer: ShardType::Cartographer,
            key_cache: vec![],
            storage_path,
        }
    }

    fn new_inheritor_shard(
        key_count: u64,
        filter_starting_len: usize,
        filter_starting_len_mult: usize,
        storage_path: PathBuf,
    ) -> Self {
        let n = Self::expected_n(filter_starting_len, *REHASH_THRESHOLD.get().unwrap());
        let hash_family_size = Self::optimal_k(filter_starting_len, n);

        Self {
            filter: bitvec![0; filter_starting_len],
            key_count,
            bloom_length: filter_starting_len,
            bloom_length_mult: filter_starting_len_mult,
            hash_family_size: hash_family_size,
            filter_layer: ShardType::Inheritor,
            key_cache: vec![],
            storage_path,
        }
    }
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum ShardType {
    Cartographer,
    Inheritor,
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

const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

// JumpConsistentHash function, rust port from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
// Original algorithm founded in 2014 by google Lamping & Veach
pub fn jump_hash_partition(key: u64, buckets: usize) -> usize {
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

    b as usize
}

pub const HASH_SEED_SELECTION: [u64; 6] = [
    0x8badf00d, 0xdeadbabe, 0xabad1dea, 0xdeadbeef, 0xcafebabe, 0xfeedface,
];


#[derive(Debug)]
pub struct BloomFilterConfig {
    pub rehash: Option<bool>, // True by default
    pub throughput: Option<Throughput>, // 12 
    pub accuracy: Option<Accuracy>, // 15.0
    pub initial_capacity: Option<Capacity>, // 12
    // NEW FEATURES ~ TO_DO()!!
    // pub cascade_tiers: Option<Tiers> Options: 1, 2, 3
    // pub persistence: Option<bool>
    // pub file_path: Option<String/PathBuf>  needed for persistence
}

// Shard count
#[derive(Debug, Clone, Copy)]
pub enum Throughput {
    Low,
    Medium,
    High,
}

// Rehash threshold
#[derive(Debug, Clone, Copy)]
pub enum Accuracy {
    Low,
    Medium,
    High,
}

// Initial filter length
#[derive(Debug, Clone, Copy)]
pub enum Capacity {
    Low,
    Medium,
    High,
    VeryHigh
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            rehash: Some(true),
            throughput: Some(Throughput::Medium),
            accuracy: Some(Accuracy::Medium),
            initial_capacity: Some(Capacity::Medium)
        }
    }
}

impl BloomFilterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rehash(mut self, rehash: bool) -> Self {
        self.rehash = Some(rehash);
        self
    }
    
    pub fn with_throughput(mut self, volume: Throughput) -> Self {
        self.throughput = Some(volume);
        self
    }

    pub fn with_accuracy(mut self, accuracy: Accuracy) -> Self {
        self.accuracy = Some(accuracy);
        self
    }

    pub fn with_initial_capacity(mut self, capacity: Capacity) -> Self {
        self.initial_capacity = Some(capacity);
        self
    }

    pub fn rehash(&self) -> bool {
        self.rehash.unwrap_or(true)
    }

    pub fn throughput(&self) -> Throughput {
        self.throughput.unwrap_or(Throughput::Medium)
    }

    pub fn accuracy(&self) -> Accuracy {
        self.accuracy.unwrap_or(Accuracy::Medium)
    }

    pub fn initial_capacity(&self) -> Capacity {
        self.initial_capacity.unwrap_or(Capacity::Medium)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, time::Duration};

    use bincode::config::Config;
    use once_cell::sync::Lazy;
    use serde::Serialize;
    use serde::{Deserialize};
    use bincode;

    use crate::{Accuracy, BloomFilterConfig, Capacity, PerfectBloomFilter, Throughput};

    static COUNT: i32 = 2_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init().ok();
    });

    #[derive(Hash, Serialize, Deserialize, Debug)]
    struct Tester {
        name: String,
        age: u32,
    }

    #[test]
    fn test_insert_and_contains_function() {
        Lazy::force(&TRACING);

        match std::fs::remove_dir_all("./data/pbf_data/") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }

        let config = BloomFilterConfig::new()
            .with_rehash(true)
            .with_throughput(Throughput::Medium)
            .with_accuracy(Accuracy::Medium)
            .with_initial_capacity(Capacity::Medium);

        tracing::info!("Creating PerfectBloomFilter instance");
        let pf = PerfectBloomFilter::new_with_config(config);

        let b_c = bincode::config::standard();

        tracing::info!("PerfectBloomFilter created successfully");

        tracing::info!("Contains & insert & contains check");
        for i in 0..COUNT {
            let s = Tester{
                name: format!("Hello_{}", i),
                age: i as u32,
            };

            let hash_of_s = bincode::serde::encode_to_vec(s, b_c).unwrap();
            let g = i.to_be_bytes();
            let key_str = i.to_string();
            let key_bytes = key_str.as_bytes();
            let was_present_a = pf.contains(&key_bytes);

            if was_present_a {
                tracing::warn!("Fasle positive: {:?}", key_str);
            }

            assert_eq!(was_present_a, false);
            pf.insert(&key_bytes);
            let was_present_b = pf.contains(&key_bytes);
   
            if !was_present_b {
                tracing::warn!("Fasle negative: {:?}", key_str);
            }

            assert_eq!(was_present_b, true);
        }
    }
}
