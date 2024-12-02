use std::alloc::Layout;
use std::ops::{Bound, RangeBounds};
use crate::error::Result;

// 抽象存储引擎的定义，接入不同的存储引擎，目前支持基于内存和简单磁盘的KV存储
pub trait Engine {
    // 自定义的迭代器，用于在实现scan的时候，会返回这个迭代器
    type EngineIterator<'a>: EngineIterator where Self: 'a;
    // 设置 key/value
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    // 获取 key 对应的数据
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;
    // 删除key对应的数据，如果key不存在则忽略
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;
    
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>)-> Self::EngineIterator<'_>;
    
    // 前缀扫描，扫描只带有这个前缀的数据
    fn scan_prefix(&mut self, prefix: Vec<u8>)-> Self::EngineIterator<'_>{
        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();
        // 比如说传入aaaa，则要扫描到aaab结束，刚好比传入的最后一位+1的位置结束。
        if let Some(last) = bound_prefix.iter_mut().last(){
            *last += 1;
        };
        let end  = Bound::Excluded(bound_prefix);
        self.scan((start, end))
    }
}

// 自定义迭代器，并且支持双向遍历，DoubleEndedIterator表示支持双向遍历。这样子实现这个trait的类型就能够双向遍历了。
pub trait EngineIterator:DoubleEndedIterator<Item=Result<(Vec<u8>, Vec<u8>)>> {}


#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::{
        error::Result,
        storage::{memory::MemoryEngine},
    };
    use std::{ops::Bound, path::PathBuf};

    // 测试点读的情况
    fn test_point_opt(mut eng: impl Engine) -> Result<()> {
        // 测试获取一个不存在的 key
        let a = eng.get(b"not exist".to_vec())?;
        assert_eq!(eng.get(b"not exist".to_vec())?, None);

        // 获取一个存在的 key
        let b =  b"aa".to_vec();
        let c = vec![1, 2, 3, 4];
        // 可以看到这里创建一个vec的话，默认推断成了i32类型，而set需要的是u8类型，所以在set的时候，其实编译器将传入的
        // 1，2，3，4推断为了u8，如果把参数改为vec![1234, 2, 3, 4]，则会报错，1234超过了u8的表示范围
        eng.set(b"aa".to_vec(), vec![1, 2, 3, 4])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![1, 2, 3, 4]));

        // 重复 put，将会覆盖前一个值
        eng.set(b"aa".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"aa".to_vec())?, Some(vec![5, 6, 7, 8]));

        // 删除之后再读取
        eng.delete(b"aa".to_vec())?;
        assert_eq!(eng.get(b"aa".to_vec())?, None);

        // key、value 为空的情况
        assert_eq!(eng.get(b"".to_vec())?, None);
        eng.set(b"".to_vec(), vec![])?;
        assert_eq!(eng.get(b"".to_vec())?, Some(vec![]));

        eng.set(b"cc".to_vec(), vec![5, 6, 7, 8])?;
        assert_eq!(eng.get(b"cc".to_vec())?, Some(vec![5, 6, 7, 8]));
        Ok(())
    }

    // 测试扫描
    fn test_scan(mut eng: impl Engine) -> Result<()> {
        eng.set(b"nnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"amhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"meeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"uujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"anehe".to_vec(), b"value5".to_vec())?;

        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"e".to_vec());

        let mut iter = eng.scan((start.clone(), end.clone()));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"amhue".to_vec());

        let (key2, _) = iter.next().expect("no value founded")?;
        assert_eq!(key2, b"anehe".to_vec());
        drop(iter);

        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());
        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key3, b"uujeh".to_vec());

        let (key4, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key4, b"nnaes".to_vec());

        let (key5, _) = iter2.next_back().expect("no value founded")?;
        assert_eq!(key5, b"meeae".to_vec());

        Ok(())
    }

    // 测试前缀扫描
    fn test_scan_prefix(mut eng: impl Engine) -> Result<()> {
        eng.set(b"ccnaes".to_vec(), b"value1".to_vec())?;
        eng.set(b"camhue".to_vec(), b"value2".to_vec())?;
        eng.set(b"deeae".to_vec(), b"value3".to_vec())?;
        eng.set(b"eeujeh".to_vec(), b"value4".to_vec())?;
        eng.set(b"canehe".to_vec(), b"value5".to_vec())?;
        eng.set(b"aanehe".to_vec(), b"value6".to_vec())?;

        let prefix = b"ca".to_vec();
        let mut iter = eng.scan_prefix(prefix);
        let (key1, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key1, b"camhue".to_vec());
        let (key2, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key2, b"canehe".to_vec());
        let k1= iter.next();
        println!("{:?}", k1);
        let ke2 = iter.next().transpose()?;
        println!("{:?}", ke2);

        Ok(())
    }

    #[test]
    fn test_memory() -> Result<()> {
        //test_point_opt(MemoryEngine::new())?;
        //test_scan(MemoryEngine::new())?;
        test_scan_prefix(MemoryEngine::new())?;
        Ok(())
    }
}
