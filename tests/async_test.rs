#[cfg(test)]
mod tests {
    use std::{io::Read, time::Duration};

    use bincode;
    use bincode::config::Config;
    use once_cell::sync::Lazy;
    use serde::Deserialize;
    use serde::Serialize;

    use perfect_bloomfilter::perfect_bloomfilter::r#async as usync;
    use perfect_bloomfilter::perfect_bloomfilter::config::*;

    static COUNT: i32 = 30_000_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_thread_names(true)
            .init();
    });

    #[derive(Hash, Serialize, Deserialize, Debug)]
    struct Tester {
        name: String,
        age: u32,
    }

    #[tokio::test]
    async fn async_test_insert_and_contains_function() {
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
        let pf = usync::PerfectBloomFilter::new_with_config(config);

        let b_c = bincode::config::standard();

        tracing::info!("PerfectBloomFilter created successfully");

        tracing::info!("Contains & insert & contains check");
        for i in 0..COUNT {

            /*
             let s = Tester {
                name: format!("Hello_{}", i),
                age: i as u32,
            };
             */
           

            //let hash_of_s = bincode::serde::encode_to_vec(s, b_c).unwrap();
            //let g = i.to_be_bytes();
            let key_str = i.to_string();
            let key_bytes = key_str.as_bytes();
            let was_present_a = pf.contains(&key_bytes).await;

            if was_present_a {
                tracing::warn!("Fasle positive: {:?}", key_str);
            }

            assert_eq!(was_present_a, false);

            let _ = pf.insert(&key_bytes).await.map_err(|e| {
                tracing::info!("Failed to insert key: {}, Error: {}", key_str, e)
            });
            let was_present_b = pf.contains(&key_bytes).await;

            if !was_present_b {
                tracing::warn!("Fasle negative: {:?}", key_str);
            }

            assert_eq!(was_present_b, true);
        }
    }
}
