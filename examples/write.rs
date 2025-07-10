use motec_i2::{ChannelMetadata, Datatype, Header, I2Result, LDWriter, Sample};
use std::fs::{self, File};
use std::io::{BufReader, BufRead, Cursor};
use std::path::Path;

fn main() -> I2Result<()> {
    for entry in fs::read_dir(".").expect("Failed to read current directory") {
        let entry = entry.expect("Invalid directory entry");
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "csv" {
                let filename = path.file_name().unwrap().to_string_lossy();
                println!("\n--- Processing: {} ---", filename);
                process_csv_file(&path)?;
            }
        }
    }
    for entry in fs::read_dir(".").expect("Failed to read current directory") {
        let entry = entry.expect("Invalid directory entry");
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "csv" {
                println!("Deleting CSV file: {:?}", path.file_name().unwrap());
                if let Err(e) = fs::remove_file(&path) {
                    eprintln!("Failed to delete file {:?}: {}", path, e);
                }
            }
        }
    }
    Ok(())
}

fn process_csv_file(path: &Path) -> I2Result<()> {
    let stem = path.file_stem().unwrap().to_string_lossy();
    let output_filename = format!("{}.ld", stem);
    let cleaned_csv_filename = format!("cleaned_{}.csv", stem);

    // Step 1: Read and clean CSV
    let raw_file = File::open(path).expect("Cannot open CSV");
    let reader = BufReader::new(raw_file);
    let mut time_values: Vec<f64> = Vec::new();
    let mut cleaned_lines: Vec<String> = Vec::new();

    for (i, line) in reader.lines().enumerate() {
        if let Ok(line) = line {
            let mut parts: Vec<&str> = line.trim_end_matches(',').split(',').collect();

            if i > 0 {
                // Data row: try to parse TIME
                if let Some(time_str) = parts.first() {
                    if let Ok(time) = time_str.trim().parse::<f64>() {
                        time_values.push(time);
                    }
                }
            }

            // Remove TIME column
            if !parts.is_empty() {
                parts.remove(0);
            }

            cleaned_lines.push(parts.join(","));
        }
    }


    let cleaned_csv = cleaned_lines.join("\n");
    fs::write(&cleaned_csv_filename, &cleaned_csv).expect("Failed to save cleaned CSV");


    let mut rdr = csv::Reader::from_reader(Cursor::new(cleaned_csv));
    let headers = rdr.headers().expect("Cannot read headers").clone();

    let records: Vec<Vec<String>> = rdr.records()
        .filter_map(|r| r.ok())
        .map(|r| r.iter().map(|s| s.trim().to_string()).collect())
        .collect();

    let avg_dt = if time_values.len() > 1 {
        let total_time = time_values.last().unwrap() - time_values.first().unwrap();
        total_time / (time_values.len() - 1) as f64
    } else {
        0.0
    };

    let sample_rate = if avg_dt > 0.0 {
        (1.0 / avg_dt).round() as u16
    } else {
        1
    };

    println!("Detected sample rate: {} Hz", sample_rate);


    // Step 3: Remove TIME column
    let headers_vec: Vec<String> = headers.iter().map(|s| s.to_string()).collect();

    let data_columns: Vec<Vec<Sample>> = (0..headers.len())
        .map(|col_idx| {
            records.iter()
                .filter_map(|row| row.get(col_idx))
                .filter_map(|val| val.parse::<f32>().ok())
                .map(Sample::F32)
                .collect()
        })
        .collect();

    // Step 4: Channel metadata
    let mut channel_metas = Vec::new();
    for header in &headers_vec {
        let meta = ChannelMetadata {
            prev_addr: 0,
            next_addr: 0,
            data_addr: 0,
            data_count: 0,
            datatype: Datatype::F32,
            sample_rate: sample_rate,
            offset: 0,
            mul: 1,
            scale: 1,
            dec_places: 0,
            name: header.to_string(),
            short_name: header.chars().take(8).collect(),
            unit: "".to_string(),
        };
        channel_metas.push(meta);
    }

    // Step 5: Write .ld file
    let mut file = File::create(&output_filename).expect("Failed to create output file");

    let header = Header {
        channel_meta_ptr: 13384,
        channel_data_ptr: 23056,
        event_ptr: 1762,
        device_serial: 12007,
        device_type: "ADL".to_string(),
        device_version: 420,
        num_channels: 1,
        date_string: "23/11/2005".to_string(),
        time_string: "09:53:00".to_string(),
        driver: "".to_string(),
        vehicleid: "".to_string(),
        venue: "".to_string(),
        session: "".to_string(),
        short_comment: "".to_string(),
    };

    let mut writer = LDWriter::new(&mut file, header);
    for (meta, samples) in channel_metas.into_iter().zip(data_columns.into_iter()) {
        writer = writer.with_channel(meta, samples);
    }

    writer.write()?;
    println!("Saved: {}", output_filename);
    Ok(())
}
