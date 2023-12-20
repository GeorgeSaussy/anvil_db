#[cfg(test)]
mod test {

    use std::cmp::Ordering;
    use std::sync::mpsc::channel;

    use crate::common::cmp_key;
    use crate::context::Context;
    use crate::context::SimpleContext;
    use crate::helpful_macros::unwrap;
    use crate::kv::TombstonePair;
    use crate::kv::TombstonePointReader;
    use crate::kv::TryTombstoneScanner;
    use crate::logging::debug;
    use crate::logging::DefaultLogger;
    use crate::sst::block_cache::cache::LruBlockCache;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::reader::SstReader;
    use crate::sst::writer::SstWriteSettings;
    use crate::sst::writer::SstWriter;
    use crate::storage::blob_store::BlobStore;
    use crate::storage::blob_store::InMemoryBlobStore;
    use crate::storage::blob_store::ReadCursor;

    // end to end tests

    #[test]
    fn test_sst_end_to_end_memory() {
        let store = InMemoryBlobStore::new();
        test_sst_end_to_end(store);
    }

    fn test_sst_end_to_end<BS: Clone + BlobStore>(store: BS) {
        // TODO(t/1388): Should `top` be bigger?
        let top: u64 = 1_000;
        let max_len = 100;

        // make data
        let mut keys = Vec::new();
        let mut values = Vec::new();

        // RNG set up
        let a = 11113;
        let b = 43237;
        let mut z = b % a;

        for k in 0..top {
            let key = k.to_be_bytes();
            keys.push(key);
            let mut value_data = Vec::new();
            z += b;
            z %= a;
            let value_len = z % max_len;
            for _ in 0..value_len {
                z += b;
                z %= a;
                value_data.push((z % (0xFF + 1)) as u8);
            }
            values.push(value_data);
        }

        let (compactor_tx, _compactor_rx) = channel();

        // test writer
        let writer = SstWriter::new(
            store.clone(),
            SstWriteSettings::default().set_writing_minor_sst(true),
        );
        let mut pairs: Vec<Result<TombstonePair, ()>> = Vec::with_capacity(top as usize);
        for k in 0..top {
            let l = k as usize;
            pairs.push(Ok(TombstonePair::new(keys[l].to_vec(), values[l].clone())));
        }
        let blob_ids = writer
            .write_all(pairs.into_iter())
            .expect("could not write all");
        assert_eq!(blob_ids.len(), 1);
        let sst_blob_id = &blob_ids[0];
        {
            let mut buf = vec![0; 200];
            let mut read_cursor = store
                .read_cursor(sst_blob_id)
                .expect("could not get read cursor");
            read_cursor
                .read_exact(&mut buf)
                .expect("could not read from cursor");
            debug!("starting bytes in SST file: {:?}", buf);
        }

        let ctx = SimpleContext::from((
            store.clone(),
            LruBlockCache::with_capacity(32),
            DefaultLogger::default(),
        ));
        // test scan
        let reader = unwrap!(SstReader::new(
            ctx.blob_store_ref(),
            sst_blob_id,
            compactor_tx
        ));
        let mut scanner = unwrap!(reader.try_scan(&ctx));
        let pair = scanner.start().expect("could not start scanner");
        assert_eq!(cmp_key(pair.key_ref(), &keys[0]), Ordering::Equal);
        let value_ref = pair.value_ref();
        assert!(
            value_ref.as_ref().is_some(),
            "found pair: {:?}, but should have found key {:?} and value {:?}",
            pair,
            keys[0],
            values[0]
        );
        assert_eq!(
            cmp_key(value_ref.as_ref().unwrap(), &values[0]),
            Ordering::Equal,
            "pair value: {:?}, values[0]: {:?}",
            pair.value_ref(),
            values[0]
        );
        for k in 1..top {
            let l = k as usize;
            let pair = scanner.next().unwrap().expect("could not get next pair");
            assert_eq!(cmp_key(pair.key_ref(), &keys[l]), Ordering::Equal);
            assert_eq!(
                cmp_key(pair.value_ref().as_ref().unwrap(), &values[l]),
                Ordering::Equal
            );
        }

        // test point reads
        let top: usize = 100; // may not test the full range since point reads are slow.

        for _ in 0..2 {
            let k = 0;
            let option = reader.get(&ctx, &keys[k]).expect("could not get key");
            let value = option.unwrap();
            match value.as_ref() {
                Some(value) => {
                    assert_eq!(cmp_key(value, &values[k]), Ordering::Equal)
                }
                None => continue,
            }
        }

        for k in 1..top {
            let option = reader.get(&ctx, &keys[k]).expect("could not get key");
            let value = option.unwrap();
            match value.as_ref() {
                Some(value) => {
                    assert_eq!(cmp_key(value, &values[k]), Ordering::Equal)
                }
                None => continue,
            }
        }
    }
}
