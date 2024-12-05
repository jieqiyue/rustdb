use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine, keycode::serialize_key},
};

use super::{Engine, Transaction};

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
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    // 1. 插入数据的数据类型校验，非空校验。
    // 2. 主键冲突校验。
    // 3. 调用mvcc层接口保存行数据。
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 校验行的有效性
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )))
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )))
                }
                _ => {}
            }
        }

        // 从传入要插入的那一行数据中找到主键的值
        let pk = table.get_primary_key(&row)?;
        // 查看主键对应的数据是否已经存在了
        let id = Key::Row(table_name.clone(), pk.clone()).encode()?;
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                pk, table_name
            )));
        }

        // 存放数据
        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)?;

        Ok(())
    }
    
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 更新了主键，则删除旧的数据
        if *id != new_pk {
            let key = Key::Row(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(key)?;
        }

        // 然后直接将新的列设置到存储引擎当中，如果主键没有被修改，那就还是set原来那一列，而对同一个key进行set之后，
        // 会进行覆盖。所以原来的数据就被更新了。而如果是主键进行了修改的话，那原来的那一列因为是用到了主键作为key的编码的，
        // 所以当主键被修改了之后，就要先把原来的主键的那一行给删除掉去。不然原来那一行还是存在的。
        let key = Key::Row(table.name.clone(), new_pk).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)?;

        Ok(())
    }
    
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()> {
        let key = Key::Row(table.name.clone(), id.clone()).encode()?;
        self.txn.delete(key)
    }
    
    // 1. 调用mvcc Transaction获取到整个表的数据，然后进行过滤返回
    fn scan_table(
        &self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>> {
        let table = self.must_get_table(table_name.clone())?;
        // 在SQL引擎这一层在创建一行数据的时候，就是使用的表名+主键来作为唯一的值，所以这里要把所有的行扫出来也是需要传入
        // 表名作为前缀，就能扫描到所有的行了。
        let prefix = KeyPrefix::Row(table_name).encode()?;
        let results = self.txn.scan_prefix(prefix)?;

        let mut rows = Vec::new();
        for result in results {
            // 过滤数据，目前只支持简单的表达式，所以这里直接判断值是否相等，而不是大于小于。
            // row在这一层进行create_row的时候，传入的value就是Row进行编码过后的，所以这里进行了bincode的解码。
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                rows.push(row);
            }
        }
        
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }

        // 判断表的有效性
        table.validate()?;

        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name).encode()?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key{
    Table(String),
    // 元组第一项是表名，第二项是一个主键
    Row(String, Value),
}
impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}
impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'c', 3);")?;

        let v = s.execute("update t1 set b = 'aa' where a = 1;")?;
        let v = s.execute("update t1 set a = 33 where a = 3;")?;
        println!("{:?}", v);

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
