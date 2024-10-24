use serde::{Deserialize, Serialize};
use crate::sql::engine::{Engine, Session, Transaction};
use crate::error::{Error, Result};
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::storage::{self, engine::Engine as StorageEngine};

pub struct KVEngine<E: StorageEngine> {
    pub kv:storage::mvcc::Mvcc<E>,
}
impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self{kv:self.kv.clone()}
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction定义，实际上对存储引擎中MvccTransaction的封装
pub struct KVTransaction<E: StorageEngine> {
    txn:storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn:storage::mvcc::MvccTransaction<E>) -> Self {
        Self{txn} 
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())    
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        todo!()
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否存在
        if self.get_table(table.name.clone())?.is_some(){
            return Err(Error::Internal(format!("table {} already exists", table.name)));  
        }
        
        // 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("table {} has no columns", table.name)));
        }
        
        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        
        self.txn.set(bincode::serialize(&key)?, value)
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self.txn.get(bincode::serialize(&key)?)?
            .map(|v|bincode::deserialize(&v))
            .transpose()?
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key{
    Table(String),
    Row(String, Value),
}