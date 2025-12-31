use std::{clone, process::exit, sync::Arc};

use anyhow::Result;
use bincode2::deserialize;
use lz4_flex::decompress_size_prepended;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut rng = StdRng::from_os_rng();
    let port = rng.random_range(6000..9000);
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Netflow>>(1000);
    tokio::spawn(listen_port(port, tx));
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::channel::<Netflow>(1000);

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let workers = std::cmp::max(1, cores / 2);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    for _ in 0..workers {
        let rx = Arc::clone(&rx);
        let processed_tx = processed_tx.clone();

        tokio::spawn(async move {
            loop {
                let next_chunk = { rx.lock().await.recv().await };
                let Some(chunk) = next_chunk else { break };

                validate_chunk(chunk, processed_tx.clone()).await;
            }
        });
    }
    let mut validated_netflows = Box::new(Vec::<Netflow>::new());
    tokio::spawn(async move {
        while let Some(netflow) = processed_rx.recv().await {
            validated_netflows.push(netflow);
        }
    });

    register_processor(port).await?;
    tokio::signal::ctrl_c().await.unwrap();
    Ok(())
}

async fn validate_chunk(
    netflow_chunk: Vec<Netflow>,
    processed_tx: tokio::sync::mpsc::Sender<Netflow>,
) {
    for netflow in netflow_chunk {
        if !netflow.is_valid() {
            continue;
        }
        if let Err(err) = processed_tx.send(netflow).await {
            eprintln!("failed to send processed netflow: {}", err);
            break;
        }
    }
}

impl Netflow {
    fn is_valid(&self) -> bool {
        self.src_ip.is_some()
            && self.dst_ip.is_some()
            && self.protocol.is_some()
            && self.bytes.is_some()
            && self.packets.is_some()
            && self.start_ts.is_some()
            && self.end_ts.is_some()
            && self.src_asn.is_some()
            && self.dst_asn.is_some()
    }
}

async fn register_processor(port: i32) -> Result<()> {
    match TcpStream::connect("0.0.0.0:8080").await {
        Ok(mut stream) => {
            stream
                .write_all(format!("connect {}", port).as_bytes())
                .await?;
        }
        Err(err) => {
            println!("error connecting to distributor: {}", err);
            exit(1);
        }
    }
    Ok(())
}

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

async fn listen_port(
    port: i32,
    tx: tokio::sync::mpsc::Sender<Vec<Netflow>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut prefix = [0u8; 5];
                if socket.read_exact(&mut prefix).await.is_err() {
                    break;
                }

                if &prefix == b"chunk" {
                    let mut len_buf = [0u8; 4];
                    if socket.read_exact(&mut len_buf).await.is_err() {
                        break;
                    }
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut compressed = vec![0u8; len];
                    if socket.read_exact(&mut compressed).await.is_err() {
                        break;
                    }

                    match decompress_size_prepended(&compressed) {
                        Ok(decompressed) => match deserialize::<Vec<Netflow>>(&decompressed) {
                            Ok(items) => {
                                println!("received {} netflow items", items.len());

                                if let Err(err) = tx.send(items).await {
                                    eprintln!("failed to send netflow chunk: {}", err);
                                }
                            }
                            Err(err) => eprintln!("failed to deserialize: {}", err),
                        },
                        Err(err) => eprintln!("failed to decompress: {}", err),
                    }
                } else if &prefix[..5] == b"healt" {
                    let mut check_rest = [0u8; 6];
                    if socket.read_exact(&mut check_rest).await.is_err() {
                        break;
                    }
                    let _ = socket.write_all(b"healthy").await;
                } else {
                    break;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use bincode2::serialize;
    use lz4_flex::compress_prepend_size;

    use super::*;

    #[tokio::test]
    async fn test_listen_port() {
        let port = 7001;
        let (tx, _rx) = tokio::sync::mpsc::channel::<Vec<Netflow>>(100);
        tokio::spawn(listen_port(port, tx));

        let mut stream = TcpStream::connect(format!("0.0.0.0:{}", port))
            .await
            .unwrap();
        stream.write_all(b"health check").await.unwrap();
        let mut response = [0u8; 7];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"healthy");
    }
    #[tokio::test]
    async fn test_listen_chunk() {
        let port = 7002;
        let (tx, _rx) = tokio::sync::mpsc::channel::<Vec<Netflow>>(100);
        tokio::spawn(async move {
            listen_port(port, tx).await.unwrap();
        });

        let mut stream = TcpStream::connect(format!("0.0.0.0:{}", port))
            .await
            .unwrap();
        let netflows = vec![Netflow {
            flow_id: 1,
            src_ip: Some("192.168.1.1".into()),
            dst_ip: Some("192.168.1.2".into()),
            src_port: Some(80),
            dst_port: Some(443),
            protocol: Some(6),
            bytes: Some(1024),
            packets: Some(5),
            start_ts: Some(1678886400),
            end_ts: Some(1678886500),
            src_asn: Some(12345),
            dst_asn: Some(54321),
        }];
        let serialized = serialize(&netflows).unwrap();
        let compressed = compress_prepend_size(&serialized);
        let decompressed = decompress_size_prepended(&compressed).unwrap();
        let items: Vec<Netflow> = deserialize(&decompressed).unwrap();
        assert_eq!(items[0].flow_id, 1);
        let mut chunk_message = b"chunk".to_vec();
        let len = (compressed.len() as u32).to_be_bytes();
        chunk_message.extend_from_slice(&len);
        chunk_message.extend_from_slice(&compressed);
        stream.write_all(&chunk_message).await.unwrap();
    }
}
