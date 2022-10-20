use std::error::Error;
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
pub struct DecodeError {
    pub msg: String,
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid decoding")
    }
}

impl Error for DecodeError {}

pub fn read_string_from_bytes(input: &[u8]) -> String {
    // we have to check if we have a valid utf8 representation and if we do
    // we return the value if not we return a DecodeError
    if let Some(last) = input.to_vec().iter().rev().position(|&pos| pos != 0) {
        return String::from_utf8_lossy(&input[0..input.len() - last]).to_string();
    }

    // use case when all the bytes are set to 0
    "".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_token_name_32_bytes() {
        let expected_name = "Maker";
        let name_bytes: &[u8; 32] = &[
            77, 97, 107, 101, 114, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];

        assert_eq!(expected_name, read_string_from_bytes(name_bytes));
    }

    #[test]
    fn test_read_token_symbol_32_bytes() {
        let expected_name = "MKR";
        let name_bytes: &[u8; 32] = &[
            77, 75, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ];

        assert_eq!(expected_name, read_string_from_bytes(name_bytes));
    }

    #[test]
    fn test_read_string_from_bytes32_all_zeros() {
        let bytes: &[u8; 32] = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        assert_eq!("".to_string(), read_string_from_bytes(bytes));
    }

    #[test]
    fn test_read_string_from_bytes64_all_zeros() {
        let bytes: &[u8; 64] = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];

        assert_eq!("".to_string(), read_string_from_bytes(bytes));
    }

    #[test]
    fn test_read_string_from_bytes32_empty_bytes() {
        let bytes: &[u8; 0] = &[];

        assert_eq!("".to_string(), read_string_from_bytes(bytes));
    }
}
