use btleplug::api::{Central, Manager as _, Peripheral, ScanFilter};
use btleplug::platform::Manager;
use chrono::{Local, DateTime, Utc};
use log::{info, error};
use std::error::Error;
use tokio::time::Duration;
use arrow::array::{StringArray, Int32Array, StructArray, ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::ByteStream;
use tokio_util::codec::{BytesCodec, FramedRead};
use std::sync::Arc;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    info!("Starting BLE advertisement scanner...");

    let manager = Manager::new().await?;
    let adapter_list = manager.adapters().await?;
    
    if adapter_list.is_empty() {
        error!("No Bluetooth adapters found");
        return Err("No Bluetooth adapters found".into());
    }

    let central = adapter_list.into_iter().next().unwrap();
    info!("Using adapter: {}", central.adapter_info().await?);

    central.start_scan(ScanFilter::default()).await?;
    info!("Scanning for BLE advertisements...");

    let start_time = Instant::now();
    let duration = Duration::from_secs(15 * 60); // 15 minutes

    let mut timestamps = Vec::new();
    let mut names = Vec::new();
    let mut address_types = Vec::new();
    let mut addresses = Vec::new();
    let mut rssis = Vec::new();
    let mut manufacturer_ids = Vec::new();
    let mut manufacturer_data = Vec::new();

    while start_time.elapsed() < duration {
        let peripherals = central.peripherals().await?;
        
        for peripheral in peripherals.iter() {
            if let Ok(properties) = peripheral.properties().await {
                if let Some(props) = properties {
                    let timestamp: DateTime<Utc> = Utc::now();
                    let address = props.address.to_string();
                    let address_type = format!("{:?}", props.address_type);
                    let name = props.local_name.unwrap_or_default();
                    let rssi = props.rssi.unwrap_or(0);
                    
                    for (id, data) in props.manufacturer_data {
                        timestamps.push(timestamp.timestamp_nanos());
                        names.push(name.clone());
                        address_types.push(address_type.clone());
                        addresses.push(address.clone());
                        rssis.push(rssi);
                        manufacturer_ids.push(id as i32);
                        manufacturer_data.push(data.iter().map(|b| format!("{:02X}", b)).collect::<String>());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        Field::new("name", DataType::Utf8, false),
        Field::new("address_type", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("rssi", DataType::Int32, false),
        Field::new("manufacturer_id", DataType::Int32, false),
        Field::new("manufacturer_data", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(address_types)) as ArrayRef,
            Arc::new(StringArray::from(addresses)) as ArrayRef,
            Arc::new(Int32Array::from(rssis)) as ArrayRef,
            Arc::new(Int32Array::from(manufacturer_ids)) as ArrayRef,
            Arc::new(StringArray::from(manufacturer_data)) as ArrayRef,
        ],
    )?;

    let file = File::create("ble_data.arrow")?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    let file = File::open("ble_data.arrow").await?;
    let stream = FramedRead::new(file, BytesCodec::new());
    let byte_stream = ByteStream::from(stream);

    client.put_object()
        .bucket("your-bucket-name")
        .key("ble_data.arrow")
        .body(byte_stream)
        .send()
        .await?;

    info!("Data uploaded to Cloudflare R2");

    Ok(())
}
