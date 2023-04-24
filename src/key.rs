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
pub fn split(key: &String, index: usize) -> String {
    return try_split(key, index).unwrap();
}
pub fn try_split(key: &String, index: usize) -> Option<String> {
    let val = key.split(":").nth(index);
    match val {
        Some(val) => Some(val.to_string()),
        None => None,
    }
}
pub fn last(key: &String) -> &str {
    return try_last(key).unwrap();
}
pub fn try_last(key: &String) -> Option<&str> {
    let val = key.split(":").last();
    match val {
        Some(val) => Some(val),
        None => None,
    }
}
