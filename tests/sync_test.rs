
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
mod tests {
    use std::{io::Read, time::Duration};
    use once_cell::sync::Lazy;

    // Bloom filter imports
    use perfect_bloomfilter::config::*;
    use perfect_bloomfilter::filter::PerfectBloomFilter;


    static COUNT: i32 = 10_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_thread_names(true)
            .init();
    });

    #[test]
    fn sync_test_insert_and_contains_function() {
        Lazy::force(&TRACING);

        match std::fs::remove_dir_all("./data/pbf_data/") {
            Ok(_) => tracing::info!("Deleted pbf data"),
            Err(e) => tracing::warn!("Failed to delete PBF data: {e}"),
        }

        let config = BloomFilterConfig::new()
            .with_rehash(true)
            .with_throughput(Throughput::Medium)
            .with_accuracy(Accuracy::Medium)
            .with_initial_capacity(Capacity::Medium)
            .with_worker_cores(Workers::Cores1);

        tracing::info!("Creating PerfectBloomFilter instance");
        let pf = PerfectBloomFilter::new_with_config(config);

      

        tracing::info!("Contains & insert & contains check");
        for i in 0..COUNT {
            let key_str = i.to_string();
            let key_bytes = key_str.as_bytes();
            let was_present_a = pf.contains(&key_bytes); // Initial existence check

            if was_present_a {
                tracing::warn!("Fasle positive: {:?}", key_str);
            }

            assert_eq!(was_present_a, false);

            let _ = pf.insert(&key_bytes).map_err(|e| { 
                tracing::info!("Failed to insert key: {}, Error: {}", key_str, e)
            });
            let was_present_b = pf.contains(&key_bytes); // Confirmation existence check

            if !was_present_b {
                tracing::warn!("Fasle negative: {:?}", key_str);
            }

            assert_eq!(was_present_b, true);
        }       
    }
}
