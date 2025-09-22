
#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;
    use anyhow::Result;

    use once_cell::sync::Lazy;

    use crate::internals::PerfectBloomFilter;

    static COUNT: i32 = 1_500_000;

    static TRACING: Lazy<()> = Lazy::new(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
    });

     #[test]
    fn test_filter_loop_sync() -> Result<()> {
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
        let pf = PerfectBloomFilter::system();

        tracing::info!("PerfectBloomFilter created successfully");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_and_insert(&key)?;
            /*
            if i % 100_000 == 0 {
                std::thread::sleep(Duration::from_millis(500));
            }
             */
            

            if was_present {
                tracing::error!("Insertion Phase Failed at key {}", i);
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, false);

        }

        //let _ = std::thread::sleep(Duration::from_secs(3));
        tracing::info!("Starting confirmation phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            let was_present = pf.contains_and_insert(&key)?;

            if !was_present {
                tracing::error!("Confirmation Phase Failed at key {}", i);
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");


        
        Ok(())
    }
}