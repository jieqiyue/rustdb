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

// 执行结果集，不同的节点执行结果是不同的
#[derive(Debug)]
pub enum ResultSet{
    CreateTable {
        table_name: String,
    },

    Insert {
        // 表示插入了多少行数据
        count:usize,
    },

    Scan{
        column: Vec<String>,
        rows:Vec<Row>,
    }
}

// 通用的执行器的定义trait，有不同的执行器来实现这个trait
pub trait Executor<T:Transaction> {
    // 定义执行器的execute方法，然后比如说有Insert的执行器，有Create Table的执行器。
    // 这些不同的执行器有不同的逻辑，比如说Create Table就需要先判断当前数据库中是否已经有这个表了，如果没有再去创建这个表。
    // 那么这个逻辑就是在执行器里面做的，对于底层的存储引擎，就是提供一些基础的扫描，创建行等的接口，具体的逻辑还是要在
    // 执行器层来做的。
    fn execute(self:Box<Self>, txn:&mut T) -> Result<ResultSet>;
}

impl<T:Transaction> dyn Executor<T> {
    // 由于Executor是一个trait，所以这里得用Box来包裹。
    pub fn build(node :Node)->Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert { table_name, columns, values } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}