use std::io::Write;
use std::time::Instant;

use anvil_db::AnvilDB;
extern crate anvil_db;

const KEY_SIZE_BYTES: usize = 16;

fn next_pair(pair: &mut [u8; 32]) -> (&[u8], &[u8]) {
    let primes: [u8; 32] = [
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89,
        97, 97, 101, 103, 107, 109, 113, 127,
    ];

    for i in 0..pair.len() {
        pair[i] = pair[i].wrapping_add(primes[i]);
    }

    let key = &pair[0..KEY_SIZE_BYTES];
    let val = &pair[KEY_SIZE_BYTES..32];
    (key, val)
}

/// The goal of this test is to see how long it would take to write out 1GB
/// of raw key-value pairs using a single thread as opposed to doing the same
/// with AnvilDB with default settings. The performance should be similar,
/// and get closer as the number of key-value pairs increases. This is because
/// AnvilDB may take a few iterations before it realizes that there is no
/// advantage to batching WAL writes because the client application must be
/// single-threaded.
fn main() {
    let mb = 1024 * 1024;
    let n_pairs = mb / (2 * KEY_SIZE_BYTES);

    let path = "/tmp/anvil_gb_dump";
    if std::fs::remove_dir_all(path).is_ok() {
        println!("Cleaned up old test directory.");
    }
    std::fs::create_dir(path).unwrap();

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() <= 1 || args[1] != "skip-raw" {
        let mut pair = [0_u8; 2 * KEY_SIZE_BYTES];
        let mut raw_writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{path}/dump.dat"))
            .unwrap();
        let start = Instant::now();
        for _ in 0..n_pairs {
            let (key, val) = next_pair(&mut pair);
            raw_writer.write_all(key).unwrap();
            raw_writer.write_all(val).unwrap();
        }
        let duration = start.elapsed();
        println!(
            "Writing {} key-value to a flat file took {:.2} sec ({:.2} ms per pair)",
            n_pairs,
            duration.as_secs_f64(),
            duration.as_micros() as f64 / n_pairs as f64
        );
    }
    let anvil_path = format!("{path}/anvil");
    std::fs::create_dir(&anvil_path).unwrap();

    let mut pair = [0_u8; 2 * KEY_SIZE_BYTES];
    let db = AnvilDB::new(&anvil_path).unwrap();
    let start = Instant::now();
    for _ in 0..n_pairs {
        let (key, val) = next_pair(&mut pair);
        db.set(key, val).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "Writing {} key-value pairs to AnvilDB took {:.2} sec ({:.2} ms per pair)",
        n_pairs,
        duration.as_secs_f64(),
        duration.as_micros() as f64 / n_pairs as f64
    );

    std::fs::remove_dir_all(path).unwrap();
}
