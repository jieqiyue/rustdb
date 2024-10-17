use crate::sql::executor::{Executor, ResultSet};
use crate::sql::schema::Table;
use crate::error::Result;

// 创建表
pub struct CreateTable {
    schema: Table
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl Executor for CreateTable {
    fn execute(&self) -> Result<super::ResultSet> {
        todo!()
    }
}