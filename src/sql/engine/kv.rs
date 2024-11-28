use serde::{Deserialize, Serialize};
use crate::sql::engine::{Engine, Session, Transaction};
use crate::error::{Error, Result};
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::storage::{self, engine::Engine as StorageEngine};

// 这里使用KVEngine实现上层定义的Engine，使用KVTransaction实现上层定义的Transaction

// ToDo kvEngine为什么没有new方法？
pub struct KVEngine<E: StorageEngine> {
    pub kv:storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self{kv:self.kv.clone()}
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    // ToDo 这个地方self.kv.begin()，这个self的kv是什么时候传入的？是创建Session的时候传入的吗
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
        let table = self.must_get_table(table_name.clone())?;
        
        // 检验SQL语句中插入的值的类型和表定义的时候列的类型是否匹配
        for (i, col) in table.columns.iter().enumerate(){
            match row[i].datatype() {
                None if col.nullable =>{},
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                },
                Some(dt) if dt != col.datatype =>{
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                },
                _ => {}
            }
        }
        
        // 存放数据，以第一列作为唯一标识，表名+第一列作为key，然后这一行作为值
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;
        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
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

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        let res = s.execute("select * from t1;")?;
       // println!("{:?}", res);

        Ok(())
    }
}