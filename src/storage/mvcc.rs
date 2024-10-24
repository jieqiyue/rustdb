use std::collections::btree_map::Keys;
use std::sync::{Arc, Mutex};
use crate::storage::engine::Engine;
use crate::error::Result;
use crate::sql::types::Value;

pub struct Mvcc<E:Engine>{
    // 由于Engine并不是线程安全的实现，但是调用的时候可能并发调用，所以这里要用线程安全的Mutex保护。
    engine: Arc<Mutex<E>>
}

// mvcc是支持事务的。目前是直接调用storage里面的engine的存储接口。
// 所以这个mvcc其实是engine的上层。
impl<E:Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self{engine: self.engine.clone()}
    }
}

impl<E:Engine> Mvcc<E> {
    pub fn new(end:E) -> Self {
        Self{engine:Arc::new(Mutex::new(end))}
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>>{
        Ok(MvccTransaction::begin(self.engine.clone()))
    }
}

pub struct MvccTransaction<E:Engine>{
    engine: Arc<Mutex<E>>,
}

impl<E:Engine> MvccTransaction<E> {
    pub fn begin(eng:Arc<Mutex<E>>) -> Self{
        Self{
            engine:eng
        }
    }

    pub fn commit(&self) -> Result<()>{
        Ok(())
    }

    pub fn rollback(&self) -> Result<()>{
        Ok(())
    }

    pub fn set(&self, key:Vec<u8>, value: Vec<u8>) -> Result<()>{
        let mut eng = self.engine.lock()?;
        eng.set(key, value)
    }
    
    pub fn get(&self, key:Vec<u8>) -> Result<Option<Vec<u8>>>{
        let mut eng = self.engine.lock()?;
        eng.get(key)
    }
    
    // 前缀扫描，因为用的是kv存储引擎，所有表的数据都是存放在一块的，所以为了区分不同的表的数据，需要把表名作为前缀。
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>>{
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        
        let mut result = Vec::new();
        
        while let Some((key, value)) = iter.next().transpose()?{
            result.push(ScanResult{key, value});
        }
        
        Ok(result)
    }
}

pub struct ScanResult{
    pub key: Vec<u8>, 
    pub value: Vec<u8>,
}
