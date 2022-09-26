use crate::pb::entity::entity_change::Operation;
use crate::pb::entity::value::Typed;
use crate::pb::entity::{EntityChange, Field, Value};
use crate::{utils, EntityChanges};
use std::str;
use substreams::pb::substreams::StoreDelta;

fn convert_i32_to_operation(operation: i32) -> Operation {
    return match operation {
        x if x == Operation::Unset as i32 => Operation::Unset,
        x if x == Operation::Create as i32 => Operation::Create,
        x if x == Operation::Update as i32 => Operation::Update,
        x if x == Operation::Delete as i32 => Operation::Delete,
        _ => panic!("unhandled operation: {}", operation),
    };
}

impl EntityChanges {
    //todo: not sure about the clone, maybe there is a better
    // way to do this
    pub fn push_change(
        &mut self,
        entity: &str,
        id: String,
        ordinal: u64,
        operation: Operation,
    ) -> &mut EntityChange {
        let entity_change = EntityChange::new(entity, id, ordinal, operation);
        self.entity_changes.push(entity_change);
        return self.entity_changes.last_mut().unwrap();
    }
}

impl EntityChange {
    pub fn new(entity: &str, id: String, ordinal: u64, operation: Operation) -> EntityChange {
        EntityChange {
            entity: entity.to_string(),
            id,
            ordinal,
            operation: operation as i32,
            fields: vec![],
        }
    }

    pub fn new_bigdecimal_field_change(
        &mut self,
        field_name: &str,
        new_value: String,
    ) -> &mut EntityChange {
        self.fields.push(Field {
            name: field_name.to_string(),
            new_value: Some(Value {
                typed: Some(Typed::Bigdecimal(new_value)),
            }),
            old_value: None,
        });

        self
    }

    pub fn update_bigdecimal(&mut self, name: &str, delta: StoreDelta) -> &mut EntityChange {
        let old_value: String = utils::decode_bytes_to_big_decimal(delta.old_value).to_string();
        let new_value: String = utils::decode_bytes_to_big_decimal(delta.new_value).to_string();
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn new_bigint_field_change(
        &mut self,
        field_name: &str,
        new_value: String,
    ) -> &mut EntityChange {
        self.fields.push(Field {
            name: field_name.to_string(),
            new_value: Some(Value {
                typed: Some(Typed::Bigint(new_value)),
            }),
            old_value: None,
        });

        self
    }

    pub fn update_bigint(&mut self, name: &str, delta: StoreDelta) -> &mut EntityChange {
        let old_value: String = utils::decode_bytes_to_big_int(delta.old_value).to_string();
        let new_value: String = utils::decode_bytes_to_big_int(delta.new_value).to_string();
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bigint(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn update_bigint_from_values(
        &mut self,
        name: &str,
        old_value: String,
        new_value: String,
    ) -> &mut EntityChange {
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bigint(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn new_int32_field_change(&mut self, name: &str, new_value: i32) -> &mut EntityChange {
        self.fields.push(Field {
            name: name.to_string(),
            new_value: Some(Value {
                typed: Some(Typed::Int32(new_value)),
            }),
            old_value: None,
        });
        self
    }

    pub fn update_int32(&mut self, name: String, delta: StoreDelta) -> &mut EntityChange {
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
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Int32(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Int32(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Int32(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn new_string_field_change(
        &mut self,
        field_name: &str,
        new_value: String,
    ) -> &mut EntityChange {
        self.fields.push(Field {
            name: field_name.to_string(),
            new_value: Some(Value {
                typed: Some(Typed::String(new_value)),
            }),
            old_value: None,
        });
        self
    }

    // WARN: also here, check for nullability when the input string is empty in the Delta
    pub fn update_string(&mut self, name: String, delta: StoreDelta) -> &mut EntityChange {
        let old_value: String = str::from_utf8(delta.old_value.as_slice())
            .unwrap()
            .to_string();
        let new_value: String = str::from_utf8(delta.new_value.as_slice())
            .unwrap()
            .to_string();
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::String(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::String(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::String(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn update_bytes(&mut self, name: String, delta: StoreDelta) -> &mut EntityChange {
        let operation: Operation = convert_i32_to_operation(self.operation);
        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Bytes(delta.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bytes(delta.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Bytes(delta.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn update_bool(&mut self, name: String, delta: StoreDelta) -> &mut EntityChange {
        let old_value: bool = !delta.old_value.contains(&(0 as u8));
        let new_value: bool = !delta.new_value.contains(&(0 as u8));
        let operation: Operation = convert_i32_to_operation(self.operation);

        match operation {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Bool(new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bool(old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name,
                new_value: Some(Value {
                    typed: Some(Typed::Bool(new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }
}
