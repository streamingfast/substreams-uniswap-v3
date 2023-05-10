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

pub fn time_as_i64_address_as_str(key: &String) -> (i64, &str) {
    return (segment(key, 1).parse::<i64>().unwrap(), segment(key, 2));
}

pub fn pool_windows_id_fields(key: &String) -> (&str, &str, &str) {
    let table_name = first_segment(key);
    let time_id = segment(key, 1);
    let pool_address = segment(key, 2);

    return (table_name, time_id, pool_address);
}
