#[macro_use]
extern crate nom;


use std::fmt;
use nom::*;
use std::path::Path;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::collections::HashMap;

fn read_bytes<R: Read+Seek>(input: &mut R, start: u64, length: usize) -> Result<Vec<u8>, String> {
    //println!("Want to read from start={} for length={} bytes to end={}", start, length, start+length as u64);
    let mut res_vec = vec![0; length];

    try!(input.seek(SeekFrom::Start(start)).map_err(|_| "couldn't seek".to_string()));

    try!(input.read_exact(&mut res_vec).map_err(|_| "Couldn't read bytes".to_string()));
    Ok(res_vec)
}

named!(parse_header<(i32, i16, i16)>,
   do_parse!(
       take!(1) >>   // level?
       take!(3) >>   // date last modified
       num_recs: le_i32         >>
       bytes_in_header: le_i16  >>
       bytes_in_rec: le_i16     >>
       take!(2) >>  // res. fill w/ zero
       take!(1) >>  // flag: incomplete transaction
       take!(1) >>  // encryption flag
       take!(12) >> // res. multi-user proc.
       take!(1) >>  // prod. mdx flag
       take!(1) >>  // lang. drv. id
       take!(2) >>  // res.

       ( (num_recs, bytes_in_header, bytes_in_rec) )
   )
);

fn parse_field_name(i: &[u8]) -> String {
    // TODO this accepts UTF8, when it should only accept ASCII
    ::std::str::from_utf8(i).unwrap().trim_right_matches('\x00').to_string()
}

named!(parse_field_descriptor<FieldHeader>,
    do_parse!(
        // FIXME use convert name to String here
        name: take!(11) >>
        field_type: take!(1) >>
        take!(4) >>      // res.
        field_length: be_u8 >>
        decimal_count: be_u8 >>
        take!(2) >>      // work area ID
        take!(1) >>      // ex.
        take!(10)>>      // res.
        take!(1) >>      // prod. mdx flag
        ({
            let field_type = ::std::str::from_utf8(field_type).unwrap().to_string().remove(0);
            let field_type = match field_type {
                'N' => FieldType::Numeric,
                'C' => FieldType::Character,
                _ => { panic!("Unknown char {:?}", field_type) },
            };

            FieldHeader{
                name: parse_field_name(name),
                field_type: field_type,
                field_length: field_length,
                decimal_count: decimal_count,
           }
        })
    )
);

#[derive(Debug)]
pub struct DbfFile<R: Read+Seek> {
    _dbf_file_handle: R,
    _fields: Vec<FieldHeader>,
    _num_recs: u32,
    _bytes_in_rec: u16,
}

pub struct DbfRecordIterator<R: Read+Seek> {
    _dbf_file: DbfFile<R>,
    _next_rec: u32,
}


#[derive(Debug, Clone)]
pub enum FieldType {
    Character,
    Numeric,
    // FIXME more types
}

#[derive(Debug)]
pub enum Field {
    Character(String),
    Numeric(f64),
    Null,
    // FIXME more types
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Field::Character(ref s) => write!(f, "{}", s),
            &Field::Numeric(ref n) => write!(f, "{}", n),
            &Field::Null => write!(f, "(NULL)"),
        }
    }
}


#[derive(Debug, Clone)]
pub struct FieldHeader {
    pub name: String,
    pub field_type: FieldType,
    pub field_length: u8,
    pub decimal_count: u8,
}

pub type Record = HashMap<String, Field>;

impl DbfFile<File> {
    pub fn open_file(filename: &Path) -> Self {
        let dbf_file = File::open(filename).unwrap();
        DbfFile::open(dbf_file)
    }
}

impl<R> DbfFile<R> where R: Read+Seek {
    pub fn open(mut dbf_file: R) -> Self where R: Read+Seek {
        let header_bytes = read_bytes(&mut dbf_file, 0, 32).unwrap();
        let (num_recs, bytes_in_header, bytes_in_rec) = parse_header(&header_bytes).to_result().unwrap();
        // -1 is for the \x0D separator
        // last -1 is maybe because of an off by one error? FIXME
        let num_headers = (bytes_in_header - 1) / 32 - 1;

        let fields: Vec<_> = read_bytes(&mut dbf_file, 32, (num_headers*32) as usize).unwrap().chunks(32).map(|b| parse_field_descriptor(b).to_result().unwrap()).collect();

        DbfFile{ _dbf_file_handle: dbf_file, _fields: fields, _num_recs: num_recs as u32, _bytes_in_rec: bytes_in_rec as u16 }
    }

    pub fn record(&mut self, rec_id: u32) -> Option<Record> {
        if rec_id >= self._num_recs {
            return None;
        }

        let header_length = (32 + 32 * self._fields.len() + 2) as u64;
        let bytes = read_bytes(&mut self._dbf_file_handle, header_length + (rec_id as u64*self._bytes_in_rec as u64), self._bytes_in_rec as usize).or_else(|e| {
            if rec_id == self._num_recs - 1 {
                // If there's an error and it's the last record, then for some reason it works if
                // we take one byte less.
                read_bytes(&mut self._dbf_file_handle, header_length + (rec_id as u64*self._bytes_in_rec as u64), self._bytes_in_rec as usize - 1)
            } else {
                Err(e)
            }
        }).unwrap();
        let mut offset: usize = 0;
        let mut fields = HashMap::with_capacity(self._fields.len());

        for field in self._fields.iter() {
            let this_field_bytes: Vec<_> = bytes.iter().skip(offset).take(field.field_length as usize).map(|x| x.clone()).collect();
            offset = offset + field.field_length as usize;

            let this_field_ascii = String::from_utf8(this_field_bytes).unwrap().trim().to_owned();

            // Is this field a Character
            // FIXME gotta be a better way to do this
            let is_char = match field.field_type { FieldType::Character => true, _ => false };

            // Spec says that a string '*' means NULL, but empty strings are also viewed as null by
            // some software
            let is_null = this_field_ascii.chars().nth(0) == Some('*') || (this_field_ascii.len() == 0 && is_char );

            let value = if is_null {
                Field::Null
            } else {
                match field.field_type {
                    FieldType::Character => Field::Character(this_field_ascii),
                    FieldType::Numeric => Field::Numeric(this_field_ascii.parse().unwrap()),
                }
            };

            fields.insert(field.name.clone(), value);
        }

        Some(fields)
    }

    pub fn records(self) -> DbfRecordIterator<R> {
        DbfRecordIterator{ _dbf_file: self, _next_rec: 0 }
    }

    pub fn num_records(&self) -> u32 {
        return self._num_recs
    }

    pub fn headers(&self) -> &Vec<FieldHeader> {
        &self._fields
    }
}

impl<R> DbfRecordIterator<R> where R: Read+Seek {
    pub fn into_inner(self) -> DbfFile<R> {
        self._dbf_file
    }
}

impl<R> Iterator for DbfRecordIterator<R> where R: Read+Seek {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self._next_rec >= self._dbf_file._num_recs {
            None
        } else {
            let rec = self._dbf_file.record(self._next_rec);
            self._next_rec = self._next_rec + 1;
            rec
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self._dbf_file._num_recs as usize, Some(self._dbf_file._num_recs as usize))
    }
}
