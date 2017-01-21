extern crate dbf;

use dbf::*;

use std::path::Path;
use std::env::args;

pub fn main() {
    let filename = args().nth(1).expect("filename missing");
    let mut dbffile = DbfFile::open_file(&Path::new(&filename));
    
    let headers = dbffile.headers().clone();
    for (idx, hdr) in headers.iter().enumerate() {
        let field_type = match hdr.field_type {
            FieldType::Character => "String",
            FieldType::Numeric => {
                if hdr.decimal_count == 0 {
                    "Integer"
                } else {
                    "Double"
                }
            }
        };
        println!("Field {idx}: Type={field_type}, Title=`{name}', Width={field_length}, Decimals={decimal_count}", idx=idx, name=hdr.name, field_type=field_type, field_length=hdr.field_length, decimal_count=hdr.decimal_count);
    }

    for rec_id in 0..dbffile.num_records() {
        print!("\n");
        println!("Record: {}", rec_id);
        let rec = dbffile.record(rec_id).unwrap();
        for header in headers.iter() {
            println!("{}: {}", header.name, rec[&header.name]);
        }
    }

}
