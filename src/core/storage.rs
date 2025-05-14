pub mod value;

use value::Value;

pub trait StorageManager {
    fn find(&self, key: &str) -> Option<Value> {
        None
    }
}
