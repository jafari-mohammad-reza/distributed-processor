use std::sync::Arc;

use tokio::{fs, sync::mpsc::Sender};

use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

use crate::Netflow;

pub struct DB {
    db_url: String,
    pool: Pool<Postgres>,
}

impl DB {
    pub async fn new(db_url: String) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(6)
            .connect(&db_url)
            .await?;

        Ok(Self {
            db_url: db_url,
            pool,
        })
    }
    pub async fn create_table(&self) -> Result<(), sqlx::Error> {
        match sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS netflow (
            flow_id BIGINT,
            src_ip TEXT,
            dst_ip TEXT,
            src_port INT,
            dst_port INT,
            protocol SMALLINT,
            bytes BIGINT,
            packets BIGINT,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            src_asn INT,
            dst_asn INT
        )
        "#,
        )
        .execute(&self.pool)
        .await
        {
            Ok(res) => Ok(()),
            Err(e) => Err(e),
        }
    }
    pub async fn insert_data(&self, sql_path: &str) -> Result<(), sqlx::Error> {
        let sql = fs::read_to_string(sql_path).await?;
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn read_chunk(
        self: Arc<Self>,
        chunk_size: usize,
        page: usize,
        tx: Sender<Vec<Netflow>>,
    ) -> Result<(), sqlx::Error> {
        let offset = page * chunk_size;
        let batch_size = 1000;
        let mut remaining = chunk_size;

        while remaining > 0 {
            let current_batch = batch_size.min(remaining);

            let rows = match sqlx::query_as::<_, Netflow>(
                r#"
    SELECT flow_id, src_ip, dst_ip, src_port, dst_port,
           protocol, bytes, packets,
           EXTRACT(EPOCH FROM start_ts)::BIGINT AS start_ts,
           EXTRACT(EPOCH FROM end_ts)::BIGINT AS end_ts,
           src_asn, dst_asn
    FROM netflow
    ORDER BY flow_id
    LIMIT $1 OFFSET $2
    "#,
            )
            .bind(current_batch as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => return Err(e),
            };

            if let Err(e) = tx.send(rows).await {
                eprintln!("Failed to send batch: {}", e);
            }

            remaining -= current_batch;
        }

        Ok(())
    }
}
