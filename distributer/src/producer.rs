use crate::Netflow;
use lz4_flex::compress_prepend_size;
use std::io::{Error, Read};
use std::os::linux::net::TcpStreamExt;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
pub struct Producer {
    processors: Arc<Mutex<Vec<String>>>,
    curr_index: Arc<Mutex<usize>>,
}

impl Producer {
    pub fn new() -> Self {
        Self {
            processors: Arc::new(Mutex::new(Vec::new())),
            curr_index: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn listen_processor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("0.0.0.0:8080").await?;
        println!("Listening on port 8080...");

        let processors: Arc<Mutex<Vec<String>>> = Arc::clone(&self.processors);

        loop {
            let (mut socket, addr) = listener.accept().await?;
            println!("New connection from {}", addr);

            let processors = Arc::clone(&processors);

            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];

                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let cmd = String::from_utf8_lossy(&buf[..n]).to_string();

                            if cmd == "connect" {
                                let mut procs = processors.lock().unwrap();
                                if !procs.contains(&addr.ip().to_string()) {
                                    procs.push(addr.ip().to_string());
                                }
                            } else if cmd == "disconnect" {
                                let mut procs = processors.lock().unwrap();
                                procs.retain(|ip| ip != &addr.ip().to_string());
                            } else {
                                if let Err(e) = socket.write_all(b"invalid command").await {
                                    eprintln!("Failed to write to socket: {}", e);
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to read from socket: {}", e);
                            break;
                        }
                    }
                }

                println!("Connection from {} closed", addr);
            });
        }
    }

    pub async fn heartbeat_processors(&self) -> Result<(), Error> {
        let processors: Arc<Mutex<Vec<String>>> = Arc::clone(&self.processors);
        let mut procs = processors.lock().unwrap();
        for processor in procs.iter() {
            let mut stream = TcpStream::connect(processor).await?;
            stream.write_all("health-check".as_bytes()).await?;
            let mut buf = vec![0; 1024];
            let n = stream.read(&mut buf).await?;
            let resp = String::from_utf8_lossy(&buf[..n]).to_string();
            println!("Received: {:?}", resp);
            if resp != "healthy" {
                procs.retain(|ip| ip != processor);
            }
        }
        Ok(())
    }

    pub async fn produce(&self, items: Vec<Netflow>) -> Result<(), Error> {
        let encoded: Vec<u8> = bincode2::serialize(&items).expect("failed to encode items");
        let compressed: Vec<u8> = compress_prepend_size(&encoded);
        let len = (compressed.len() as u32).to_be_bytes();

        let processors = self.processors.lock().unwrap();
        if processors.is_empty() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                "no processors available",
            ));
        }

        let mut index_lock = self.curr_index.lock().unwrap();
        let processor_addr = &processors[*index_lock];
        *index_lock = (*index_lock + 1) % processors.len();

        let mut stream = TcpStream::connect(processor_addr).await?;
        stream.write_all(&len).await?;
        stream.write_all(&compressed).await?;
        Ok(())
    }
}
