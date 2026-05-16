//! Batched NDJSON shipping over HTTP.
//!
//! No retries on individual events: the whole batch either lands (response 200) or fails
//! (network / 5xx). The collector keeps the un-acked watermark, so the next scan re-reads
//! the same lines and the server dedupes via `INSERT OR IGNORE`.

use anyhow::Result;
use po_k_core::{Event, MachineId};
use po_k_proto::{
    BatchHeader, BatchKind, IngestResponse, SubagentMetaRow, HEADER_API_KEY, HEADER_IDEMPOTENCY_KEY,
};

pub struct Shipper {
    client: reqwest::Client,
    server_url: String,
    api_key: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ShipStats {
    pub requested: u64,
    pub accepted: u64,
    pub duplicates: u64,
}

impl Shipper {
    pub fn new(server_url: String, api_key: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("po-k_collector/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            server_url,
            api_key,
        })
    }

    pub async fn ship(&self, machine_id: &MachineId, events: &[Event]) -> Result<ShipStats> {
        if events.is_empty() {
            return Ok(ShipStats::default());
        }
        let count = events.len() as u64;
        let batch_id = uuid::Uuid::now_v7().to_string();
        let header = BatchHeader {
            kind: BatchKind::BatchHeader,
            batch_id: batch_id.clone(),
            machine_id: machine_id.clone(),
            sent_at: chrono::Utc::now().to_rfc3339(),
            count,
            team_id: None,
        };

        let mut body = Vec::with_capacity(16 * 1024);
        serde_json::to_writer(&mut body, &header)?;
        body.push(b'\n');
        for ev in events {
            serde_json::to_writer(&mut body, ev)?;
            body.push(b'\n');
        }

        let url = format!("{}/ingest", self.server_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .header(HEADER_API_KEY, &self.api_key)
            .header(HEADER_IDEMPOTENCY_KEY, &batch_id)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ingest failed: {status}: {body}");
        }
        match resp.json::<IngestResponse>().await? {
            IngestResponse::Ok { accepted, duplicates } => Ok(ShipStats {
                requested: count,
                accepted,
                duplicates,
            }),
            IngestResponse::Error { message, rejected_line } => {
                anyhow::bail!("server error: {message} (rejected_line={rejected_line:?})")
            }
        }
    }

    pub async fn ship_meta(&self, rows: &[SubagentMetaRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(2 * 1024);
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let url = format!(
            "{}/ingest/subagent-meta",
            self.server_url.trim_end_matches('/')
        );
        let resp = self
            .client
            .post(&url)
            .header(HEADER_API_KEY, &self.api_key)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("subagent-meta ingest failed: {status}: {body}");
        }
        Ok(())
    }
}
