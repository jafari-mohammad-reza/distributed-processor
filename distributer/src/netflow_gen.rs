use chrono::{Duration, NaiveDateTime};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc;
use std::thread;

pub struct NetflowGenConfig {
    pub rows: usize,
    pub null_prob: f64,
    pub seed: u64,
    pub output_path: String,
}

impl Default for NetflowGenConfig {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            null_prob: 0.15,
            seed: 0xdeadbeef,
            output_path: "netflow.sql".to_string(),
        }
    }
}


pub fn run(config: NetflowGenConfig) -> std::io::Result<()> {
    let file = File::create(&config.output_path)?;
    let mut out = BufWriter::new(file);

    writeln!(
        out,
        "INSERT INTO netflow \
        (flow_id, src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets, start_ts, end_ts, src_asn, dst_asn) VALUES"
    )?;

    let base_ts =
        NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    let threads = 4;
    let rows_per_thread = config.rows / threads;

    let (tx, rx) = mpsc::channel::<String>();

    for t in 0..threads {
        let tx = tx.clone();
        let mut rng = StdRng::seed_from_u64(config.seed + t as u64);
        let base_ts = base_ts;
        let null_prob = config.null_prob;

        thread::spawn(move || {
            let start_id = t * rows_per_thread;

            for i in 0..rows_per_thread {
                let start = base_ts + Duration::seconds(rng.random_range(0..86_400));
                let end = start + Duration::milliseconds(rng.random_range(1..5_000));

                let row = format!(
                    "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}),",
                    start_id + i + 1,
                    maybe_null(rand_ip(&mut rng), &mut rng, null_prob),
                    maybe_null(rand_ip(&mut rng), &mut rng, null_prob),
                    maybe_null(rng.random_range(1024..65535), &mut rng, null_prob),
                    maybe_null(rng.random_range(1..65535), &mut rng, null_prob),
                    maybe_null(rng.random_range(1..=255), &mut rng, null_prob),
                    maybe_null(rng.random_range(40..1_000_000), &mut rng, null_prob),
                    maybe_null(rng.random_range(1..10_000), &mut rng, null_prob),
                    maybe_null(start, &mut rng, null_prob),
                    maybe_null(end, &mut rng, null_prob),
                    maybe_null(rng.random_range(1..100_000), &mut rng, null_prob),
                    maybe_null(rng.random_range(1..100_000), &mut rng, null_prob),
                );

                tx.send(row).expect("writer dropped");
            }
        });
    }

    drop(tx);

    for row in rx {
        writeln!(out, "{}", row)?;
    }

    out.flush()?;
    Ok(())
}

fn maybe_null<T: ToString>(v: T, rng: &mut StdRng, p: f64) -> String {
    if rng.random_bool(p) {
        "NULL".to_string()
    } else {
        format!("'{}'", v.to_string())
    }
}

fn rand_ip(rng: &mut StdRng) -> String {
    format!(
        "{}.{}.{}.{}",
        rng.random_range(1..=223),
        rng.random_range(0..=255),
        rng.random_range(0..=255),
        rng.random_range(1..=254),
    )
}
