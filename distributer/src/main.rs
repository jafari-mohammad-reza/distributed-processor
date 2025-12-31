use serde::{Deserialize, Serialize};
use std::{
    env, process,
    sync::{Arc, atomic::Ordering},
};
use tokio::time::{self, Duration};

use crate::producer::Producer;

mod db;
mod netflow_gen;
mod producer;

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]

pub struct Netflow {
    pub flow_id: i64,
    pub src_ip: Option<String>,
    pub dst_ip: Option<String>,
    pub src_port: Option<i32>,
    pub dst_port: Option<i32>,
    pub protocol: Option<i16>,
    pub bytes: Option<i64>,
    pub packets: Option<i64>,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
    pub src_asn: Option<i32>,
    pub dst_asn: Option<i32>,
}
#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    const ROWS_COUNT: usize = 2_000_000;
    let create_sql = env::var("CREATE_SQL").unwrap_or_else(|_| "CREATE TABLE ...".to_string());
    let db = db::DB::new(String::from("postgres://postgres@localhost:5432/postgres")).await?;

    let producer = Arc::new(Producer::new());
    {
        let producer = producer.clone();
        tokio::spawn(async move {
            if let Err(e) = producer.listen_processor().await {
                eprintln!("Error in listen_processor: {}", e);
            }
        });
    }
    {
        let producer = producer.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(err) = producer.heartbeat_processors().await {
                    println!("failed to run heartbeat {}", err)
                };
            }
        });
    }

    if create_sql == "TRUE" {
        let config = netflow_gen::NetflowGenConfig {
            rows: ROWS_COUNT,
            null_prob: 0.2,
            seed: 1337,
            output_path: "./netflow.sql".to_string(),
        };

        netflow_gen::run(config).expect("netflow generation failed");
        match db.create_table().await {
            Ok(_) => println!("table created;"),
            Err(e) => {
                println!("failed to create the table {}", e.to_string());
                process::exit(1);
            }
        }
        match db.insert_data("netflow.sql").await {
            Ok(_) => println!("data inserted successfully;"),
            Err(e) => {
                println!("failed to insert data {}", e.to_string());
                process::exit(1);
            }
        }
    }
    let mut interval = time::interval(Duration::from_secs(5));
    let db = Arc::new(db);
    loop {
        interval.tick().await;
        if !producer.ready_to_produce.load(Ordering::Acquire) {
            continue;
        }

        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = ROWS_COUNT / cores;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Netflow>>(chunk_size);
        for i in 0..cores {
            let db = db.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                if let Err(e) = db.read_chunk(chunk_size, i, tx).await {
                    eprintln!("failed to read chunk {}: {}", i, e);
                }
            });
        }

        drop(tx);
        while let Some(items) = rx.recv().await {
            if !producer.ready_to_produce.load(Ordering::Acquire) {
                continue;
            }
            match producer.produce(items).await {
                Ok(_) => println!("chunk produced successfully"),
                Err(err) => println!("failed to produce chunk {}", err),
            }
        }
    }
}
