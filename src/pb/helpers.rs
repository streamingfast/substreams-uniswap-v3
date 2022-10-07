use crate::pb::change::{
    BigDecimalChange, BigIntChange, BoolChange, BytesChange, Int32Change, StringArrayChange,
    StringChange,
};
use crate::pb::entity::entity_change::Operation;
use crate::pb::entity::{value::Typed, Array, EntityChange, Field, Value};
use crate::EntityChanges;
use std::str;

impl From<i32> for Operation {
    fn from(delta_operation: i32) -> Self {
        match delta_operation {
            0 => Operation::Unset,
            1 => Operation::Create,
            2 => Operation::Update,
            3 => Operation::Delete,
            _ => panic!("unsupported operation"),
        }
    }
}

// ---------- EntityChanges ----------
impl EntityChanges {
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

    pub fn change_bigdecimal(&mut self, name: &str, change: BigDecimalChange) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigdecimal(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn change_bigint(&mut self, name: &str, change: BigIntChange) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bigint(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bigint(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn change_int32(&mut self, name: &str, change: Int32Change) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Int32(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Int32(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Int32(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    // WARN: also here, check for nullability when the input string is empty in the Delta
    pub fn change_string(&mut self, name: &str, change: StringChange) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::String(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::String(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::String(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    #[allow(dead_code)]
    pub fn change_bytes(&mut self, name: &str, change: BytesChange) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bytes(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bytes(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bytes(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    #[allow(dead_code)]
    pub fn change_bool(&mut self, name: &str, change: BoolChange) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bool(change.new_value)),
                }),
                old_value: Some(Value {
                    typed: Some(Typed::Bool(change.old_value)),
                }),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                new_value: Some(Value {
                    typed: Some(Typed::Bool(change.new_value)),
                }),
                old_value: None,
            }),
            _ => {}
        }

        self
    }

    pub fn change_string_array(
        &mut self,
        name: &str,
        change: StringArrayChange,
    ) -> &mut EntityChange {
        match Operation::from_i32(self.operation).unwrap() {
            Operation::Unset => panic!("this should not happen"),
            Operation::Update => self.fields.push(Field {
                name: name.to_string(),
                old_value: Some(str_vec_to_pb(change.old_value)),
                new_value: Some(str_vec_to_pb(change.new_value)),
            }),
            Operation::Create => self.fields.push(Field {
                name: name.to_string(),
                old_value: None,
                new_value: Some(str_vec_to_pb(change.new_value)),
            }),
            _ => {}
        }

        self
    }
}
fn str_vec_to_pb(items: Vec<String>) -> Value {
    let mut list: Vec<Value> = vec![];
    for item in items.iter() {
        list.push(Value {
            typed: Some(Typed::String(item.clone())),
        });
    }
    Value {
        typed: Some(Typed::Array(Array { value: list })),
    }
}
