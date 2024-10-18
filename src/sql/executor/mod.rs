use crate::sql::plan::Node;
use crate::sql::schema::Column;
use crate::sql::types::Row;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::mutation::Insert;
use crate::sql::executor::query::Scan;
use crate::sql::executor::schema::CreateTable;

mod schema;
mod mutation;
mod query;

// 执行结果集
pub enum ResultSet{
    CreateTable {
        table_name: String,
    },

    Insert {
        count:usize,
    },

    Scan{
        column: Vec<String>,
        row:Vec<Row>,
    }
}

pub trait Executor<T:Transaction> {
    fn execute(&self, txn:&mut T) -> Result<ResultSet>;
}

impl<T:Transaction> dyn Executor<T> {
    pub fn build(node :Node)->Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert { table_name, columns, values } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}