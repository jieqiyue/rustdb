use crate::sql::executor::{Executor, ResultSet};
use crate::sql::schema::Table;
use crate::error::Result;
use crate::sql::engine::Transaction;

// 创建表
pub struct CreateTable {
    schema: Table
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T:Transaction> Executor<T> for CreateTable {
    fn execute(&self, txn: &mut T) -> Result<super::ResultSet> {
        todo!()
    }
}