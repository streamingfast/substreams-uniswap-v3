use std::collections::HashMap;
use substreams::pb::substreams::store_delta::Operation::Delete;
use substreams_entity_change::change::ToField;
use substreams_entity_change::pb::entity::entity_change::Operation;
use substreams_entity_change::pb::entity::{EntityChange, EntityChanges, Field, Value};

pub struct Tables {
    // Map from table name to the primary keys within that table
    pub tables: HashMap<String, Rows>,
}

impl Tables {
    pub fn new() -> Self {
        Tables {
            tables: HashMap::new(),
        }
    }

    pub fn update_row(&mut self, table: &str, key: &str) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let row = rows.pks.entry(key.to_string()).or_insert(Row::new());
        if row.operation == Operation::Delete {
            panic!("cannot delete a row on an update operation")
        }
        row.operation = Operation::Update;
        row
    }

    pub fn delete_row(&mut self, table: &str, key: &str) -> &mut Row {
        let rows = self.tables.entry(table.to_string()).or_insert(Rows::new());
        let row = rows.pks.entry(key.to_string()).or_insert(Row::new());
        row.operation = Operation::Delete;
        row.columns = HashMap::new();
        row
    }

    // Convert Tables into an EntityChanges protobuf object
    pub fn to_entity_changes(mut self) -> EntityChanges {
        let mut entities = EntityChanges::default();
        for (table, rows) in self.tables.iter_mut() {
            for (pk, row) in rows.pks.iter_mut() {
                // Map the row.operation into an EntityChange.Operation
                let mut change = EntityChange::new(table, pk, 0, row.operation);
                for (field, value) in row.columns.iter_mut() {
                    change.fields.push(Field {
                        name: field.clone(),
                        new_value: Some(value.clone()),
                        old_value: None,
                    });
                }
                entities.entity_changes.push(change.clone());
            }
        }
        entities
    }
}

pub struct Rows {
    // Map of primary keys within this table, to the fields within
    pub pks: HashMap<String, Row>,
}

impl Rows {
    pub fn new() -> Self {
        Rows {
            pks: HashMap::new(),
        }
    }
}

pub struct Row {
    // Verify that we don't try to delete the same row as we're creating it
    pub operation: Operation,
    // Map of field name to its last change
    pub columns: HashMap<String, Value>,
    // Finalized: Last update or delete
    pub finalized: bool,
}

impl Row {
    pub fn new() -> Self {
        Row {
            operation: Operation::Unset,
            columns: HashMap::new(),
            finalized: false,
        }
    }

    // TODO: add set_bigint, set_bigdecimal which both take a bi/bd string representation
    pub fn set<N: AsRef<str>, T: ToField>(&mut self, name: N, change: T) -> &mut Self {
        if self.operation == Operation::Delete {
            panic!("cannot set fields on a delete operation")
        }
        let field = change.to_field(name);
        self.columns.insert(field.name, field.new_value.unwrap());
        self
    }

    pub fn mark_final(&mut self) -> &mut Self {
        self.finalized = true;
        self
    }
}
