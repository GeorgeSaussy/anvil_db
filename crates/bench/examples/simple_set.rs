use std::time::Instant;

use anvil_db::AnvilDB;
use bench::next_pair;
extern crate anvil_db;

fn main() {
    std::fs::create_dir("/tmp/anvil_check").unwrap();
    let db = AnvilDB::new("/tmp/anvil_check").unwrap();
    let mut pair = [0_u8; 32]; // 16 bytes for key, 16 bytes for value
    let n_pairs = 32760; // 1 MiB of key-value pairs
    let start = Instant::now();
    for _ in 0..n_pairs {
        let (key, val) = next_pair(&mut pair);
        db.set(key, val).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "Wrote {} pairs in {:.2} sec ({:.2} ms per pair)",
        n_pairs,
        duration.as_secs_f64(),
        duration.as_micros() as f64 / n_pairs as f64
    );

    std::fs::remove_dir_all("/tmp/anvil_check").unwrap();
}
