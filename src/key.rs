// pub trait KeySplit {
//     fn key_split(&self, index: usize) -> String;
//     fn try_key_split(&self, index: usize) -> Option<String>;
// }
// impl KeySplit for &DeltaProto<T> {
//     fn key_split(&self, index: usize) -> String {
//         split(&self.key, index)
//     }
//     fn try_key_split(&self, index: usize) -> Option<String> {
//         try_split(&self.key, index)
//     }
// }

use substreams::store::{Deltas, GetKey};

pub fn filter_first_segment_eq<'a, T: GetKey>(deltas: &'a Deltas<T>, val: &str) -> Vec<&'a T> {
    let mut out: Vec<&T> = vec![];
    deltas
        .deltas
        .iter()
        .filter(|delta| first_segment(delta.get_key()) == val)
        .for_each(|delta| out.push(delta));
    out
}

pub fn first_segment(key: &String) -> &str {
    key.split(":").next().unwrap()
}

/// 0-based index segment of key split using ":" as delimiter
pub fn segment(key: &String, index: usize) -> &str {
    return try_segment(key, index).unwrap();
}
pub fn try_segment(key: &String, index: usize) -> Option<&str> {
    let val = key.split(":").nth(index);
    match val {
        Some(val) => Some(val),
        None => None,
    }
}
pub fn last_segment(key: &String) -> &str {
    return try_last_segment(key).unwrap();
}
pub fn try_last_segment(key: &String) -> Option<&str> {
    let val = key.split(":").last();
    match val {
        Some(val) => Some(val),
        None => None,
    }
}
