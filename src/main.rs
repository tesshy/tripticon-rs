use btleplug::api::{Central, Manager as _, Peripheral, ScanFilter};
use btleplug::platform::Manager;
use chrono::Local;
use log::{info, error};
use std::error::Error;
use tokio::time::Duration;

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

    loop {
        let peripherals = central.peripherals().await?;
        
        for peripheral in peripherals.iter() {
            if let Ok(properties) = peripheral.properties().await {
                if let Some(props) = properties {
                    let timestamp = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f");
                    let address = props.address;
                    let address_type = props.address_type;
                    let name = props.local_name.unwrap_or_default();
                    let rssi = props.rssi.unwrap_or(0);
                    
                    // Manufacturer dataの処理
                    for (id, data) in props.manufacturer_data {
                        info!(
                            "{},{},{:?},{},{},{:04X},{}",
                            timestamp, name, address_type, address, rssi, id, data.iter().map(|b| format!("{:02X}", b)).collect::<String>()
                        );
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}