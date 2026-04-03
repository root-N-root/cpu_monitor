use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use cpu_monitor::{CpuMonitor, MonitorReport};
use reqwest::{Body, Client, multipart};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{BytesCodec, FramedRead};

#[derive(Parser, Debug)]
#[command(author, version, about = "24-hour CPU monitor for all host processes")]
struct Args {
    /// Room ID для отправки отчёта
    #[arg(short, long, default_value = "")]
    room_id: String,

    /// Интервал опроса в секундах
    #[arg(short, long, default_value = "10")]
    interval: u16,

    /// Длительность мониторинга в часах
    #[arg(long, default_value = "24")]
    duration: u8,

    /// URL для отправки отчёта
    #[arg(long, default_value = "http://10.10.0.1:9099/send-file-image")]
    webhook_url: String,
}

async fn send_report(
    webhook_url: String,
    room_id: String,
    file_path: String,
) -> anyhow::Result<String> {
    let client = Client::new();
    let file = File::open(file_path).await?;

    let stream = FramedRead::new(file, BytesCodec::new());
    let file_body = Body::wrap_stream(stream);
    let some_file = multipart::Part::stream(file_body)
        .file_name("report.txt")
        .mime_str("text/plain")?;

    let form = reqwest::multipart::Form::new()
        .text("message", "Закончен анализ процессорного времени")
        .text("room", room_id.clone())
        .part("file", some_file);

    let resp = client
        .post(webhook_url)
        .multipart(form)
        .send()
        .await
        .context("HTTP request failed")?;

    if resp.status().is_success() {
        println!("✅ Report sent to room {}", room_id);
        Ok(resp.text().await?)
    } else {
        Err(anyhow::anyhow!("Server returned: {}", resp.status()))
    }
}

async fn save_report(report: &MonitorReport, filename: &str) -> Result<()> {
    let data = serde_json::to_string_pretty(report)?;
    let mut file = File::create(filename).await?;
    file.write_all(data.as_bytes()).await?;
    println!("📄 Report saved: {}", filename);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut monitor = CpuMonitor::new(args.duration, args.interval);

    // Запуск мониторингa
    let report = monitor.prod_run_with_signal().await?;

    // Сохранение отчёта
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let report_file = format!("cpu_report_{}.json", timestamp);

    let _ = save_report(&report, &report_file).await;

    // Отправка
    println!("📤 Sending report...");
    if !args.room_id.is_empty() {
        send_report(
            args.webhook_url.clone(),
            args.room_id.clone(),
            report_file.to_string().clone(),
        )
        .await?;
    }

    println!("🏁 Done. Exiting.");
    Ok(())
}
