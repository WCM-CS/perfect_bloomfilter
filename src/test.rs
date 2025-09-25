
#[cfg(test)]
mod tests {
    use std::time::Duration;
    use anyhow::Result;

    use once_cell::sync::Lazy;

    use crate::{internals::PerfectBloomFilter,};

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
        let pf = PerfectBloomFilter::new();

        tracing::info!("PerfectBloomFilter created successfully");
        for i in 0..COUNT {
            let key = i.to_string();
            let _ = pf.insert(&key)?;

        }

        tracing::info!("Starting confirmation phase 1");
        for i in 0..COUNT {
            let key = i.to_string();
            //let was_present = pf.contains_and_insert(&key)?;
            let was_present = pf.contains(&key)?;

            if !was_present {
                tracing::error!("Confirmation Phase Failed at key {}", i);
                std::thread::sleep(Duration::from_millis(500));
            }

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");


        //dump_metadata();

        //std::thread::sleep(Duration::from_secs(10));

        Ok(())
    }

    }


