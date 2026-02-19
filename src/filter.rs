use std::{
    fs, hash::Hash, io::{BufReader, BufWriter, Read, Write}, panic, path::PathBuf, sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}
};
use crossbeam_queue::SegQueue;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tempfile::TempDir;
use anyhow::anyhow;
use crossbeam_channel::{Sender, unbounded};


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

        Ok(())
    }

    fn existence_check(&self, key: &[u8], shard_type: &ShardType) -> bool {
        let shard_vec = match shard_type {
            ShardType::Cartographer => &self.cartographer,
            ShardType::Inheritor => &self.inheritor,
        };

        let shards = self.array_sharding_hash(key, shard_type);

        for shard in shards {
            let shard = shard_vec[shard].read();
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
            let shard_guard = shard_vec[shard_idx].read();
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

        let workers: usize = match config.workers() {
            Workers::Cores1 => 1usize,
            Workers::Cores4 => 4usize,
            Workers::Cores8 => 8usize,
            Workers::HalfSysMax => {
                match std::thread::available_parallelism() {
                    Ok(cores) => cores.get(),
                    Err(_) => panic!("Failed to get sys core for max parallelism")
                }
            },
        };

        REHASH_THRESHOLD.set(threshold).ok();
        REHASH_SWITCH.set(config.rehash()).ok();

        // Rehasher threadZ
        let (tx, rx) = unbounded::<Arc<RwLock<Shard>>>();
        for _ in 0..workers {
            let thread_rx = rx.clone(); // crossbeam clonable receiver, thread save via our Arc 
            std::thread::spawn(move || {
                // Each thread waits for its shard to process
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
                carto_path,
            ))));

            inheritor.push(Arc::new(RwLock::new(Shard::new_inheritor_shard(
                0,
                1usize << filter_len_mult,
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
        // Snap current cache to disk
        let (path, new_len, new_k, layer) = {
            let s = shard_arc.write();
            s.drain(true); // Force everything currently in RAM to disk first
            (s.storage_path.clone(), s.bloom_length.load(Ordering::Relaxed) * 2, s.optimal_k_for_next_size(), s.filter_layer.clone())
        };

        //  Heavy Lifting, no locks held
        // builds new bitvec fro all the keys we wrote to the file
        let new_bitset = build_filter_from_disk(&path, new_len, new_k, &layer);

        // Catch-Up, write locked shard
        {
            let mut shard = shard_arc.write();
            
            // Take everything that arrived while we were reading Phase 2 aka temp cache
            let mut catch_up_keys = Vec::new();
            while let Some(key) = shard.key_cache.pop() {
                let hashes = static_bloom_hash(&key, new_len, new_k, &layer);
                for h in hashes {
                    let bucket = h / 64;
                    let bit_pos = h & 63; 
                    let bit_mask = 1u64 << bit_pos;
                    new_bitset[bucket].fetch_or(bit_mask, Ordering::Relaxed);
                }
                catch_up_keys.push(key);
            }

            // Apply new state
            shard.filter = new_bitset;
            shard.bloom_length.store(new_len, Ordering::SeqCst);
            shard.hash_family_size.store(new_k, Ordering::SeqCst);

            //shard.key_cache = key_backlog; // add this back to key cahce so it wil get written to disk

            for key in catch_up_keys {
                shard.key_cache.push(key);
            }


            // Persist the catch up keys immediately, log source truth transaction
            // May not be needed but ensures we are synced adnd after swap we alreday have a lock
            shard.drain(true);

            shard.is_rehashing.store(false, Ordering::Release);
        }
    }
}



pub struct Shard {
    filter: Vec<AtomicU64>,
    filter_layer: ShardType,
    key_count: AtomicU64,
    bloom_length: AtomicUsize,
    hash_family_size: AtomicUsize,
    key_cache: SegQueue<Vec<u8>>,
    storage_path: PathBuf,
    is_rehashing: AtomicBool
}

impl Shard {
    pub fn bloom_hash(&self, key: &[u8]) -> Vec<usize> {
        let hash_seeds = match self.filter_layer {
            ShardType::Cartographer => [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]],
            ShardType::Inheritor => [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]],
        };

        let hash1 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[0]);
        let hash2 = xxhash_rust::xxh3::xxh3_128_with_seed(key, hash_seeds[1]);

        let family_k_size = self.hash_family_size.load(Ordering::Relaxed);

        let mut hash_list = Vec::with_capacity(family_k_size);
        for idx in 0..family_k_size {
            let idx_u128 = idx as u128;
            let mask = (&self.bloom_length.load(Ordering::Relaxed) - 1) as u128;

            // Kirsch-Mitzenmacher optimization
            let index = hash1.wrapping_add(idx_u128.wrapping_mul(hash2)) & mask;

            hash_list.push(index as usize)
        }

        hash_list
    }

    fn bloom_insert(&self, hashes: &[usize], key: &[u8]) {
        for h in hashes {
            let bucket = h / 64; // u64 in the vec whihc holds our idx
            let bit_pos = h & 63; // bit in the u64 out hash shmacks into
            let bit_mask = 1u64 << bit_pos; // hash bit representation

            // we try to flip the bit, if its alreay flipped no cas loop, move on to next hash
            self.filter[bucket].fetch_or(bit_mask, Ordering::Relaxed);
        }

        self.key_count.fetch_add(1, Ordering::Relaxed);
        self.key_cache.push(key.to_vec());


        // hashes.iter().for_each(|hash| self.filter.set(*hash, true));
        // self.key_count += 1;
        // self.key_cache.push(key.to_vec());
    }

    fn bloom_check(&self, hashes: &[usize]) -> bool {
        hashes.iter().all(|h| {
            let bucket = h / 64; // u64 in the vec whihc holds our idx
            let bit_pos = h & 63; // bit in the u64 out hash shmacks into
            let bit_mask = 1u64 << bit_pos; // hash bit representation

            let val = self.filter[bucket].load(Ordering::Relaxed);

            (val & bit_mask) != 0
        })
    }

    fn drain(&self, force_drain: bool) {
        if self.key_cache.is_empty() { return; }
    
        let rehashing = self.is_rehashing.load(Ordering::SeqCst);
        if rehashing && !force_drain {
            return; // Stay in RAM until the rehasher is ready for Phase 3
        }

        if (self.key_cache.len() >= 50 || force_drain)
            && let Ok(file) = fs::OpenOptions::new().create(true).append(true).open(&self.storage_path) {
                let mut writer = BufWriter::new(file);
                // Pop from SegQueue instead of mem::take
                while let Some(bytes) = self.key_cache.pop() {
                    let len = bytes.len() as u32;
                    let _ = writer.write_all(&len.to_be_bytes());
                    let _ = writer.write_all(&bytes);
                }

                let _ = writer.flush();
            }
    }

    fn rehash_check(&self) -> bool {
        (self.bloom_length.load(Ordering::Relaxed) as f64 / self.key_count.load(Ordering::Relaxed) as f64) <= *REHASH_THRESHOLD.get().unwrap()
    }

    fn expected_n(new_m: usize, bits_per_key_threshold: f64) -> usize {
        (new_m as f64 / bits_per_key_threshold).floor() as usize
    }

    fn optimal_k(m: usize, n: usize) -> usize {
        ((m as f64 / n as f64) * std::f64::consts::LN_2).round() as usize
    }

    pub fn optimal_k_for_next_size(&self) -> usize {
        let new_m = self.bloom_length.load(Ordering::Relaxed) * 2;
        let expected_n = Self::expected_n(new_m, *REHASH_THRESHOLD.get().unwrap());
        Self::optimal_k(new_m, expected_n)
    }

    

    fn new_shard(
        key_count: u64,
        filter_starting_len: usize,
        storage_path: PathBuf,
        shard_type: ShardType,

    ) -> Self {
        let n = Self::expected_n(filter_starting_len, *REHASH_THRESHOLD.get().unwrap());
        let hash_family_size = Self::optimal_k(filter_starting_len, n);

        let num_buckets = filter_starting_len / 64;
        let mut filter = Vec::with_capacity(num_buckets);

        for _ in 0..num_buckets {
            filter.push(AtomicU64::new(0));
        }


        Self {
            filter,
            filter_layer: shard_type,
            key_count: AtomicU64::new(key_count),
            bloom_length: AtomicUsize::new(filter_starting_len),
            hash_family_size: AtomicUsize::new(hash_family_size),
            key_cache: SegQueue::new(),
            storage_path,
            is_rehashing: AtomicBool::new(false),
        }
    }

    pub fn new_cartographer_shard(
        key_count: u64,
        filter_starting_len: usize,
        storage_path: PathBuf,
    ) -> Self {
        Self::new_shard(key_count, filter_starting_len, storage_path, ShardType::Cartographer)
    }

    pub fn new_inheritor_shard(
        key_count: u64,
        filter_starting_len: usize,
        storage_path: PathBuf,
    ) -> Self {
        Self::new_shard(key_count, filter_starting_len, storage_path, ShardType::Inheritor)
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


fn build_filter_from_disk(path: &PathBuf, m: usize, k: usize, shard_type: &ShardType) -> Vec<AtomicU64> {
    //let mut new_filter = bitvec![0; m];
    let len = m / 64;

    //let new_filter: Vec<AtomicU64> = Vec::with_capacity(len);
    let new_filter: Vec<AtomicU64> = (0..len).map(|_| AtomicU64::new(0)).collect();

    //let new_filter: Vec<AtomicU64> = vec![AtomicU64::new(0); len];



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
            let bucket = h / 64; // u64 in the vec whihc holds our idx
            let bit_pos = h & 63; // bit in the u64 out hash shmacks into
            let bit_mask = 1u64 << bit_pos; // hash bit representation

            // we try to flip the bit, if its alreay flipped no cas loop, move on to next hash
            new_filter[bucket].fetch_or(bit_mask, Ordering::Relaxed);
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