#![feature(test)]

pub fn next_pair(pair: &mut [u8; 32]) -> (&[u8], &[u8]) {
    let primes: [u8; 32] = [
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89,
        97, 97, 101, 103, 107, 109, 113, 127,
    ];

    for i in 0..pair.len() {
        pair[i] = pair[i].wrapping_add(primes[i]);
    }

    let key = &pair[0..16];
    let val = &pair[16..32];
    (key, val)
}

#[cfg(test)]
mod test {
    extern crate test;
    use std::sync::mpsc::Sender;

    use test::Bencher;

    #[bench]
    fn bench_channel_round_trip_1(b: &mut Bencher) {
        let (tx1, rx1) = std::sync::mpsc::channel::<(usize, Sender<usize>)>();
        std::thread::spawn(move || {
            while let Ok((i, tx2)) = rx1.recv() {
                tx2.send(i).unwrap();
                if i == 0 {
                    break;
                }
            }
        });
        b.iter(|| {
            let (tx2, rx2) = std::sync::mpsc::channel();
            tx1.send((1, tx2)).unwrap();
            rx2.recv().unwrap();
        });
        let (tx2, rx2) = std::sync::mpsc::channel();
        tx1.send((0, tx2)).unwrap();
        rx2.recv().unwrap();
    }

    #[bench]
    fn bench_channel_round_trip_2(b: &mut Bencher) {
        let (tx1, rx2) = std::sync::mpsc::channel::<(usize, Sender<usize>)>();
        std::thread::spawn(move || {
            while let Ok((i, tx2)) = rx2.recv() {
                tx2.send(i).unwrap();
                if i == 0 {
                    break;
                }
            }
        });

        let (tx2, rx2) = std::sync::mpsc::channel();
        b.iter(|| {
            tx1.send((1, tx2.clone())).unwrap();
            rx2.recv().unwrap();
        });
        let (tx2, rx2) = std::sync::mpsc::channel();
        tx1.send((0, tx2)).unwrap();
        rx2.recv().unwrap();
    }
}
