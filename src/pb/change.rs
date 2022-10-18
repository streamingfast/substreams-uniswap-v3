use std::str;
use substreams::pb::substreams::StoreDelta;
use substreams::scalar::{BigDecimal, BigInt};
use substreams::store::{DeltaArray, DeltaBigDecimal, DeltaBigInt};
use substreams::Hex;

#[derive(Debug)]
pub struct Int32Change {
    pub old_value: i32,
    pub new_value: i32,
}

impl From<StoreDelta> for Int32Change {
    fn from(delta: StoreDelta) -> Self {
        let (int_bytes, _) = delta
            .old_value
            .as_slice()
            .split_at(std::mem::size_of::<i32>());
        let old_value: i32 = i32::from_be_bytes(int_bytes.try_into().unwrap());

        let (int_bytes, _) = delta
            .new_value
            .as_slice()
            .split_at(std::mem::size_of::<i32>());
        let new_value: i32 = i32::from_be_bytes(int_bytes.try_into().unwrap());
        Int32Change {
            old_value,
            new_value,
        }
    }
}

impl From<i32> for Int32Change {
    fn from(new_value: i32) -> Self {
        Int32Change {
            old_value: i32::default(),
            new_value,
        }
    }
}

// ---------- BigDecimalChange ----------
#[derive(Debug)]
pub struct BigDecimalChange {
    pub old_value: String,
    pub new_value: String,
}

impl From<StoreDelta> for BigDecimalChange {
    fn from(delta: StoreDelta) -> Self {
        BigDecimalChange {
            old_value: BigDecimal::from_store_bytes(delta.old_value).to_string(),
            new_value: BigDecimal::from_store_bytes(delta.new_value).to_string(),
        }
    }
}

impl From<DeltaBigDecimal> for BigDecimalChange {
    fn from(delta: DeltaBigDecimal) -> Self {
        BigDecimalChange {
            old_value: delta.old_value.to_string(),
            new_value: delta.new_value.to_string(),
        }
    }
}

impl From<BigDecimal> for BigDecimalChange {
    fn from(new_value: BigDecimal) -> Self {
        BigDecimalChange {
            old_value: "0".to_string(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<String> for BigDecimalChange {
    fn from(new_value: String) -> Self {
        BigDecimalChange {
            old_value: "0".to_string(),
            new_value,
        }
    }
}

// ---------- BigIntChange ----------
#[derive(Debug)]
pub struct BigIntChange {
    pub old_value: String,
    pub new_value: String,
}

impl From<StoreDelta> for BigIntChange {
    fn from(delta: StoreDelta) -> Self {
        BigIntChange {
            old_value: BigInt::from_store_bytes(delta.old_value).to_string(),
            new_value: BigInt::from_store_bytes(delta.new_value).to_string(),
        }
    }
}

impl From<BigInt> for BigIntChange {
    fn from(new_value: BigInt) -> Self {
        BigIntChange {
            old_value: "0".to_string(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<String> for BigIntChange {
    fn from(new_value: String) -> Self {
        BigIntChange {
            old_value: "0".to_string(),
            new_value,
        }
    }
}

impl From<i32> for BigIntChange {
    fn from(new_value: i32) -> Self {
        BigIntChange {
            old_value: "0".to_string(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<u32> for BigIntChange {
    fn from(new_value: u32) -> Self {
        BigIntChange {
            old_value: "0".to_string(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<u64> for BigIntChange {
    fn from(new_value: u64) -> Self {
        BigIntChange {
            old_value: "0".to_string(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<(BigInt, BigInt)> for BigIntChange {
    fn from(change: (BigInt, BigInt)) -> Self {
        BigIntChange {
            old_value: change.0.to_string(),
            new_value: change.1.to_string(),
        }
    }
}

impl From<(String, String)> for BigIntChange {
    fn from(change: (String, String)) -> Self {
        BigIntChange {
            old_value: change.0,
            new_value: change.1,
        }
    }
}

impl From<DeltaBigInt> for BigIntChange {
    fn from(delta: DeltaBigInt) -> Self {
        BigIntChange {
            old_value: delta.old_value.to_string(),
            new_value: delta.new_value.to_string(),
        }
    }
}

// ---------- StringChange ----------
#[derive(Debug)]
pub struct StringChange {
    pub old_value: String,
    pub new_value: String,
}

impl From<StoreDelta> for StringChange {
    fn from(delta: StoreDelta) -> Self {
        StringChange {
            old_value: str::from_utf8(delta.old_value.as_slice())
                .unwrap()
                .to_string(),
            new_value: str::from_utf8(delta.new_value.as_slice())
                .unwrap()
                .to_string(),
        }
    }
}

impl From<String> for StringChange {
    fn from(new_value: String) -> Self {
        StringChange {
            old_value: String::default(),
            new_value,
        }
    }
}

impl From<&String> for StringChange {
    fn from(new_value: &String) -> Self {
        StringChange {
            old_value: String::default(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<&str> for StringChange {
    fn from(new_value: &str) -> Self {
        StringChange {
            old_value: String::default(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<Hex<[u8; 20]>> for StringChange {
    fn from(new_value: Hex<[u8; 20]>) -> Self {
        StringChange {
            old_value: String::default(),
            new_value: new_value.to_string(),
        }
    }
}

impl From<i64> for StringChange {
    fn from(new_value: i64) -> Self {
        StringChange {
            old_value: String::default(),
            new_value: new_value.to_string(),
        }
    }
}

// ---------- BytesChange ----------
#[derive(Debug)]
pub struct BytesChange {
    pub old_value: Vec<u8>,
    pub new_value: Vec<u8>,
}

impl From<StoreDelta> for BytesChange {
    fn from(delta: StoreDelta) -> Self {
        BytesChange {
            old_value: Vec::from(base64::encode(delta.old_value).as_str()),
            new_value: Vec::from(base64::encode(delta.new_value).as_str()),
        }
    }
}

impl From<String> for BytesChange {
    fn from(new_value: String) -> Self {
        BytesChange {
            old_value: Vec::default(),
            new_value: Vec::from(base64::encode(new_value).as_str()),
        }
    }
}

// ---------- BoolChange ----------
#[derive(Debug)]
pub struct BoolChange {
    pub old_value: bool,
    pub new_value: bool,
}

impl From<StoreDelta> for BoolChange {
    fn from(delta: StoreDelta) -> Self {
        BoolChange {
            old_value: !delta.old_value.contains(&(0 as u8)),
            new_value: !delta.new_value.contains(&(0 as u8)),
        }
    }
}

// ---------- StringArrayChange ----------
#[derive(Debug)]
pub struct StringArrayChange {
    pub old_value: Vec<String>,
    pub new_value: Vec<String>,
}

impl From<DeltaArray<String>> for StringArrayChange {
    fn from(items: DeltaArray<String>) -> Self {
        StringArrayChange {
            old_value: items.old_value,
            new_value: items.new_value,
        }
    }
}

impl From<Vec<String>> for StringArrayChange {
    fn from(new_value: Vec<String>) -> Self {
        StringArrayChange {
            old_value: vec![],
            new_value,
        }
    }
}
