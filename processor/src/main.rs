use std::process::exit;

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
    tokio::spawn(async move { listen_port(port).await });
    register_processor(port).await?;
    tokio::signal::ctrl_c().await.unwrap();
    Ok(())
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

async fn listen_port(port: i32) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
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
                            Ok(items) => println!("received {} netflow items", items.len()),
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
