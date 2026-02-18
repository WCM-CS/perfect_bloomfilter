use std::{
    fs, 
    hash::Hash, 
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf, sync::{Arc, atomic::AtomicBool}//sync::RwLock
};
use bitvec::{bitvec, vec::BitVec};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tempfile::TempDir;
use anyhow::anyhow;
use crossbeam_channel::{Sender, Receiver, unbounded};


use crate::config::*;

static REHASH_THRESHOLD: OnceCell<f64> = OnceCell::new();
static REHASH_SWITCH: OnceCell<bool> = OnceCell::new();

pub struct PerfectBloomFilter {
    pub(crate) cartographer: Vec<Arc<RwLock<Shard>>>, // tier 1 
    pub(crate) inheritor: Vec<Arc<RwLock<Shard>>>, // tier 2
    rehash_tx: Sender<Arc<RwLock<Shard>>>,
    
    #[allow(dead_code)]
    temp_dir_handle: TempDir,
}

impl Default for PerfectBloomFilter{
    fn default() -> Self {
        Self::new()
    }
}

impl PerfectBloomFilter {
    pub fn new() -> Self {
        let config = BloomFilterConfig::default();
        Self::pbf_init(config)
    }

    pub fn new_with_config(config: BloomFilterConfig) -> Self {
        Self::pbf_init(config)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if !Self::existence_check(self, key, &ShardType::Cartographer) {
            return false;
        }

        if !Self::existence_check(self, key, &ShardType::Inheritor) {
            return false;
        }

        true
    }

    pub fn insert(&self, key: &[u8]) -> anyhow::Result<()> {
        let c_res = Self::insert_key(self, key, &ShardType::Cartographer)
            .map_err(|e| tracing::warn!("Vec {} - Insert Err: {}", ShardType::Cartographer.as_static_str(), e));

        let i_res = Self::insert_key(self, key, &ShardType::Inheritor)
            .map_err(|e| tracing::warn!("Vec {} - Insert Err: {}", ShardType::Inheritor.as_static_str(), e));

        if c_res.is_err() || i_res.is_err() {
            return Err(anyhow!("Insertion Failed"))
        }

        return Ok(())
    }

    fn existence_check(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = &shard_vec[shard].read();
            let hashes = shard.bloom_hash(key);
            let exist = shard.bloom_check(&hashes);

            if !exist {
                return false;
            }
        }

        true
    }

    fn insert_key(&self, key: &[u8], shard_type: &ShardType) -> anyhow::Result<bool> {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);
        for shard_idx in shards {
            let shard_guard = &mut shard_vec[shard_idx].write();
            let hashes = shard_guard.bloom_hash(key);
            shard_guard.bloom_insert(&hashes, key);

            if shard_guard.rehash_check() {
                // send to the receiver if it needs to rehash and is not already in reahsing mode
                if shard_guard.is_rehashing.compare_exchange(
                    false, true, 
                    std::sync::atomic::Ordering::SeqCst, 
                    std::sync::atomic::Ordering::SeqCst).is_ok() {
                        let shard_handle = Arc::clone(&shard_vec[shard_idx]);
                        let _ = self.rehash_tx.send(shard_handle);
                    }
            } else {
                shard_guard.drain(false);
            }
        }
        Ok(true)
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
        let p2 = (p1 + (mask / 2)) & mask;

        debug_assert!(p1 != p2, "Partitions must be unique");

        vec![p1, p2]
    }

    fn pbf_init(config: BloomFilterConfig) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let shard_vector_len_mult = match config.throughput() {
            Throughput::Low => 11,    // 2048
            Throughput::Medium => 12, // 4096
            Throughput::High => 13,   // 8192
        };

        let filter_len_mult = match config.initial_capacity() {
            Capacity::Low => 11,
            Capacity::Medium => 12,
            Capacity::High => 13,
            Capacity::VeryHigh => 15,
        };

        let threshold = match config.accuracy() {
            Accuracy::Low => 12.0,
            Accuracy::Medium => 15.0,
            Accuracy::High => 19.0,
        };

        REHASH_THRESHOLD.set(threshold).ok();
        REHASH_SWITCH.set(config.rehash()).ok();

        // Rehasher thread
        let (tx, rx) = unbounded::<Arc<RwLock<Shard>>>();
        for i in 0..4 {
            let thread_rx = rx.clone(); // In Crossbeam, Receiver is Cloneable!
            std::thread::spawn(move || {
                // Each thread waits for its own shard to process
                while let Ok(shard_arc) = thread_rx.recv() {
                    Self::background_rehasher_loop_logic(shard_arc);
                }
            });
        }


        let shard_vector_len = 1usize << shard_vector_len_mult;
        let mut cartographer = Vec::with_capacity(shard_vector_len);
        let mut inheritor = Vec::with_capacity(shard_vector_len);

        for shard_id in 0..shard_vector_len {
            let carto_path = base_path.join(format!(
                "{}_{}.tmp",
                ShardType::Cartographer.as_static_str(),
                shard_id
            ));
            let inheritor_path = base_path.join(format!(
                "{}_{}.tmp",
                ShardType::Inheritor.as_static_str(),
                shard_id
            ));

            cartographer.push(Arc::new(RwLock::new(Shard::new_cartographer_shard(
                0,
                1usize << filter_len_mult,
                filter_len_mult,
                carto_path,
            ))));

            inheritor.push(Arc::new(RwLock::new(Shard::new_inheritor_shard(
                0,
                1usize << filter_len_mult,
                filter_len_mult,
                inheritor_path,
            ))));
        }

        Self {
            cartographer,
            inheritor,
            rehash_tx: tx,
            temp_dir_handle: temp_dir,
        }
    }

    fn background_rehasher_loop_logic(shard_arc: Arc<RwLock<Shard>>) {
        // Phase 1: Heavy Lifting (READ LOCK ONLY FOR METADATA)

        {
            let mut s = shard_arc.write();
            s.drain(true);
        }

        let (path, new_len, new_k, layer) = {
            let s = shard_arc.read();
            (
                s.storage_path.clone(), 
                s.bloom_length * 2, 
                s.optimal_k_for_next_size(), 
                s.filter_layer.clone()
            )
        };

        let mut new_bitset = build_filter_from_disk(&path, new_len, new_k, &layer);

        // Phase 2: The Sync Swap (BRIEF WRITE LOCK)
        {
            let mut shard = shard_arc.write();
            //shard.drain(true);
            let key_backlog = std::mem::take(&mut shard.key_cache);
            
            // CATCH UP: Hash everything that entered the RAM cache while we were reading disk
            for key in &key_backlog {
                // Using a specialized hash function for the NEW dimensions
                let hashes = static_bloom_hash(key, new_len, new_k, &layer);
                for h in hashes {
                    new_bitset.set(h, true);
                }
            }

            // Commit the new filter to the shard
            shard.filter = new_bitset;
            shard.bloom_length = new_len;
            shard.bloom_length_mult += 1;
            shard.hash_family_size = new_k;
            
            // Clear the synced cache and release the rehash gate
            //shard.key_cache.clear();
            shard.is_rehashing.store(false, std::sync::atomic::Ordering::SeqCst);
        }
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
    is_rehashing: AtomicBool
}

impl Shard {
    pub fn bloom_hash_with_params(&self, key: &[u8], m: usize, k: usize) -> Vec<usize> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);

        let mut hash_list = Vec::with_capacity(k);
        let mask = (m - 1) as u128;

        for idx in 0..k {
            let index = hash1.wrapping_add((idx as u128).wrapping_mul(hash2)) & mask;
            hash_list.push(index as usize);
        }
        hash_list
    }

    pub fn bloom_hash(&self, key: &[u8]) -> Vec<usize> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);

        let mut hash_list = Vec::with_capacity(self.hash_family_size);
        for idx in 0..self.hash_family_size {
            let idx_u128 = idx as u128;
            let mask = (&self.bloom_length - 1) as u128;

            // Kirsch-Mitzenmacher optimization
            let index = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

            hash_list.push(index as usize)
        }

        hash_list
    }

    fn bloom_insert(&mut self, hashes: &[usize], key: &[u8]) {
        hashes.iter().for_each(|hash| self.filter.set(*hash, true));
        self.key_count += 1;
        self.key_cache.push(key.to_vec());
    }

    fn bloom_check(&self, hashes: &[usize]) -> bool {
        hashes.iter().all(|hash| self.filter[*hash])
    }

    fn drain(&mut self, force_drain: bool) {
        if self.key_cache.len() >= 50 || force_drain {
        
            let is_rehashing = self.is_rehashing.load(std::sync::atomic::Ordering::SeqCst);

            if is_rehashing && !force_drain {
                return;
            }

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
        (self.bloom_length as f64 / self.key_count as f64) <= *REHASH_THRESHOLD.get().unwrap()
    }

    fn expected_n(new_m: usize, bits_per_key_threshold: f64) -> usize {
        (new_m as f64 / bits_per_key_threshold).floor() as usize
    }

    fn optimal_k(m: usize, n: usize) -> usize {
        ((m as f64 / n as f64) * std::f64::consts::LN_2).round() as usize
    }

    pub fn optimal_k_for_next_size(&self) -> usize {
        let new_m = self.bloom_length * 2;
        let expected_n = Self::expected_n(new_m, *REHASH_THRESHOLD.get().unwrap());
        Self::optimal_k(new_m, expected_n)
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
            hash_family_size,
            filter_layer: ShardType::Cartographer,
            key_cache: vec![],
            storage_path,
            is_rehashing: AtomicBool::new(false)
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
            hash_family_size,
            filter_layer: ShardType::Inheritor,
            key_cache: vec![],
            storage_path,
            is_rehashing: AtomicBool::new(false)
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


fn build_filter_from_disk(path: &PathBuf, m: usize, k: usize, shard_type: &ShardType) -> BitVec {
    let mut new_filter = bitvec![0; m];
    let file = fs::File::open(path).expect("Failed to open tmp file for rehash");
    let mut reader = BufReader::new(file);
    
    let mut len_bytes = [0u8; 4];
    let mut key_buf = Vec::with_capacity(1024);

    while reader.read_exact(&mut len_bytes).is_ok() {
        let len = u32::from_be_bytes(len_bytes) as usize;
        key_buf.resize(len, 0);
        reader.read_exact(&mut key_buf).ok();

        let hashes = static_bloom_hash(&key_buf, m, k, shard_type);
        for h in hashes {
            new_filter.set(h, true);
        }
    }
    new_filter
}

fn static_bloom_hash(key: &[u8], m: usize, k: usize, layer: &ShardType) -> Vec<usize> {
    let hash_seeds = match layer {
        ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
        ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
    };
    let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
    let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);
    let mask = (m - 1) as u128;

    (0..k).map(|idx| (hash1.wrapping_add((idx as u128).wrapping_mul(hash2)) & mask) as usize).collect()
}