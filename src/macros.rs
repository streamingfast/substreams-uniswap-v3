#[macro_export]
macro_rules! new_field {
    ($name:expr, $value_type:expr, $new_value:expr) => {
        Field {
            name: $name.to_string(),
            value_type: $value_type as i32,
            new_value: $new_value,
            new_value_null: false,
            old_value: vec![],
            old_value_null: true,
        }
    };
}

#[macro_export]
macro_rules! update_field {
    ($name:expr, $value_type:expr, $old_value:expr, $new_value:expr) => {
        Field {
            name: $name.to_string(),
            value_type: $value_type as i32,
            new_value: $new_value,
            new_value_null: false,
            old_value: $old_value,
            old_value_null: false,
        }
    };
}

#[macro_export]
macro_rules! string_field_value {
    ($a:expr) => {
        $a.as_bytes().to_vec()
    };
}

#[macro_export]
macro_rules! int_field_value {
    ($a:expr) => {
        $a.to_be_bytes().to_vec()
    };
}

#[macro_export]
macro_rules! big_int_field_value {
    ($a:expr) => {
        BigInt::from_str($a.as_str())
            .unwrap()
            .to_signed_bytes_be()
            .to_vec()
    };
}

#[macro_export]
macro_rules! big_decimal_string_field_value {
    ($a:expr) => {
        $a.as_bytes().to_vec()
    };
}

#[macro_export]
macro_rules! big_decimal_vec_field_value {
    ($a:expr) => {
        $a
    };
}
