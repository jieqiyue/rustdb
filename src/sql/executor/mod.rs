use super::{engine::Transaction, plan::Node, types::Row};
use crate::error::Result;
use mutation::{Delete, Insert, Update};
use query::{Order, Scan, Offset, Limit, Projection};
use schema::CreateTable;
use join::NestedLoopJoin;

mod mutation;
mod query;
mod schema;
mod join;

// 执行结果集，不同的节点执行结果是不同的
#[derive(Debug, PartialEq)]
pub enum ResultSet{
    CreateTable {
        table_name: String,
    },

    Insert {
        // 表示插入了多少行数据
        count:usize,
    },

    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },

    Update {
        count: usize,
    },
    
    Delete {
        count: usize,
    },
}

// 通用的执行器的定义trait，有不同的执行器来实现这个trait
pub trait Executor<T:Transaction> {
    // 定义执行器的execute方法，然后比如说有Insert的执行器，有Create Table的执行器。
    // 这些不同的执行器有不同的逻辑，比如说Create Table就需要先判断当前数据库中是否已经有这个表了，如果没有再去创建这个表。
    // 那么这个逻辑就是在执行器里面做的，对于底层的存储引擎，就是提供一些基础的扫描，创建行等的接口，具体的逻辑还是要在
    // 执行器层来做的。
    // 不同的执行器，都会返回这个ResultSet
    fn execute(self:Box<Self>, txn:&mut T) -> Result<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    // 由于Executor是一个trait，所以这里得用Box来包裹。
    // build根据传入的执行节点的类型生成不同的执行器。然后执行器去执行。
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(table_name, Self::build(*source), columns),
            
            Node::Delete {
                table_name, 
                source
            } => Delete::new(table_name, Self::build(*source)),
            
            Node::Order { 
                source, 
                order_by 
            } => Order::new(Self::build(*source), order_by),
            
            Node::Limit { source, limit}  => Limit::new(Self::build(*source), limit),

            Node::Offset { source, offset }  => Offset::new(Self::build(*source), offset),

            Node::Projection { source, exprs } => Projection::new(Self::build(*source), exprs),
            
            Node::NestedLoopJoin { left, right } => {
                NestedLoopJoin::new(Self::build(*left), Self::build(*right))
            }
        }
    }
}