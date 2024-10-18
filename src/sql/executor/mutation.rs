use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, Statement};
use crate::sql::types::DataType;

pub struct Insert{
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String, columns: Vec<String>, values: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self{table_name, columns, values})
    }
}

impl<T:Transaction> Executor<T> for Insert {
    fn execute(&self, txn: &mut T) -> crate::error::Result<super::ResultSet> {
        todo!()
    }
}