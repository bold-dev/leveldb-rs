use crate::cmp::{Cmp, MemtableKeyCmp};
use crate::key_types::{build_memtable_key, parse_internal_key, parse_memtable_key, ValueType};
use crate::key_types::{LookupKey, UserKey};
use crate::types::{LdbIterator, SequenceNumber};

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use bytes::Bytes;
use integer_encoding::FixedInt;

/// Provides Insert/Get/Iterate over a sorted Vec of memtable keys.
/// MemTable uses MemtableKeys internally — key and value are packed into a single entry.
pub struct MemTable {
    entries: Rc<RefCell<Vec<Vec<u8>>>>,
    cmp: Rc<Box<dyn Cmp>>,
    approx_mem: usize,
}

impl MemTable {
    /// Returns a new MemTable, wrapping cmp inside a MemtableKeyCmp.
    pub fn new(cmp: Rc<Box<dyn Cmp>>) -> MemTable {
        MemTable::new_raw(Rc::new(Box::new(MemtableKeyCmp(cmp))))
    }

    fn new_raw(cmp: Rc<Box<dyn Cmp>>) -> MemTable {
        MemTable {
            entries: Rc::new(RefCell::new(Vec::new())),
            cmp,
            approx_mem: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    pub fn approx_mem_usage(&self) -> usize {
        self.approx_mem
    }

    pub fn add(&mut self, seq: SequenceNumber, t: ValueType, key: UserKey<'_>, value: &[u8]) {
        let memkey = build_memtable_key(key, value, t, seq);
        let added_mem = memkey.len();
        let mut entries = self.entries.borrow_mut();
        let pos = entries.partition_point(|e| self.cmp.cmp(e, &memkey) == Ordering::Less);
        entries.insert(pos, memkey);
        self.approx_mem += added_mem;
    }

    /// get returns the value for the given entry and whether the entry is marked as deleted.
    #[allow(unused_variables)]
    pub fn get(&self, key: &LookupKey) -> (Option<Bytes>, bool) {
        let entries = self.entries.borrow();
        let mkey = key.memtable_key();
        let pos = entries.partition_point(|e| self.cmp.cmp(e, mkey) == Ordering::Less);

        if pos >= entries.len() {
            return (None, false);
        }

        let foundkey = &entries[pos];
        let (fkeylen, fkeyoff, tag, vallen, valoff) = parse_memtable_key(foundkey);

        if key.user_key() == &foundkey[fkeyoff..fkeyoff + fkeylen] {
            if tag & 0xff == ValueType::TypeValue as u64 {
                return (
                    Some(foundkey[valoff..valoff + vallen].to_vec().into()),
                    false,
                );
            } else {
                return (None, true);
            }
        }
        (None, false)
    }

    pub fn iter(&self) -> MemtableIterator {
        MemtableIterator {
            entries: self.entries.clone(),
            cmp: self.cmp.clone(),
            current_idx: None,
        }
    }
}

/// MemtableIterator is an iterator over a MemTable. Key-returning methods deal with InternalKeys.
pub struct MemtableIterator {
    entries: Rc<RefCell<Vec<Vec<u8>>>>,
    cmp: Rc<Box<dyn Cmp>>,
    current_idx: Option<usize>,
}

impl LdbIterator for MemtableIterator {
    fn advance(&mut self) -> bool {
        let next_idx = match self.current_idx {
            None => 0,
            Some(i) => i + 1,
        };
        if next_idx < self.entries.borrow().len() {
            self.current_idx = Some(next_idx);
            true
        } else {
            self.current_idx = None;
            false
        }
    }

    fn reset(&mut self) {
        self.current_idx = None;
    }

    fn seek(&mut self, to: &[u8]) {
        let (_, seq, ukey) = parse_internal_key(to);
        let lkey = LookupKey::new(ukey, seq);
        let mkey = lkey.memtable_key();
        let entries = self.entries.borrow();
        let pos = entries.partition_point(|e| self.cmp.cmp(e, mkey) == Ordering::Less);
        if pos < entries.len() {
            self.current_idx = Some(pos);
        } else {
            self.current_idx = None;
        }
    }

    fn valid(&self) -> bool {
        match self.current_idx {
            None => false,
            Some(i) => i < self.entries.borrow().len(),
        }
    }

    fn current(&self) -> Option<(Bytes, Bytes)> {
        if !self.valid() {
            return None;
        }
        let entries = self.entries.borrow();
        let i = self.current_idx.unwrap();
        let key_bytes = &entries[i];
        let (keylen, keyoff, _, vallen, valoff) = parse_memtable_key(key_bytes);
        let internal_key =
            Bytes::copy_from_slice(&key_bytes[keyoff..keyoff + keylen + u64::required_space()]);
        let value = Bytes::copy_from_slice(&key_bytes[valoff..valoff + vallen]);
        Some((internal_key, value))
    }

    fn prev(&mut self) -> bool {
        loop {
            match self.current_idx {
                None => return false,
                Some(0) => {
                    self.current_idx = None;
                    return false;
                }
                Some(i) => {
                    self.current_idx = Some(i - 1);
                    let entries = self.entries.borrow();
                    let key_bytes = &entries[i - 1];
                    let (_, _, tag, _, _) = parse_memtable_key(key_bytes);
                    if tag & 0xff == ValueType::TypeValue as u64 {
                        return true;
                    }
                    // TypeDeletion: keep going back
                }
            }
        }
    }
}

/// shift_left moves s[mid..] to s[0..s.len()-mid]. The new size is s.len()-mid.
fn shift_left(s: &mut Vec<u8>, mid: usize) {
    for i in mid..s.len() {
        s.swap(i, i - mid);
    }
    let newlen = s.len() - mid;
    s.truncate(newlen);
}

#[cfg(test)]
#[allow(unused_variables)]
mod tests {
    use super::*;
    use crate::key_types::{parse_tag, truncate_to_userkey};
    use crate::options;
    use crate::test_util::{test_iterator_properties, LdbIteratorIter};
    use crate::types::current_key_val;

    fn get_memtable() -> MemTable {
        let mut mt = MemTable::new(options::for_test().cmp);
        let entries = vec![
            (ValueType::TypeValue, 115, "abc", "122"),
            (ValueType::TypeValue, 120, "abc", "123"),
            (ValueType::TypeValue, 121, "abd", "124"),
            (ValueType::TypeDeletion, 122, "abe", "125"),
            (ValueType::TypeValue, 123, "abf", "126"),
        ];

        for e in entries.iter() {
            mt.add(e.1, e.0, e.2.as_bytes(), e.3.as_bytes());
        }
        mt
    }

    #[test]
    fn test_memtable_parse_tag() {
        let tag = (12345 << 8) | 1;
        assert_eq!(parse_tag(tag), (ValueType::TypeValue, 12345));
    }

    #[test]
    fn test_memtable_add() {
        let mut mt = MemTable::new(options::for_test().cmp);
        mt.add(
            123,
            ValueType::TypeValue,
            "abc".as_bytes(),
            "123".as_bytes(),
        );

        assert_eq!(
            mt.entries.borrow()[0],
            vec![11, 97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0, 3, 49, 50, 51]
        );
        assert_eq!(
            mt.iter().next().unwrap().0,
            &[97, 98, 99, 1, 123, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_memtable_add_get() {
        let mt = get_memtable();

        // Smaller sequence number doesn't find entry
        if let Some(v) = mt.get(&LookupKey::new("abc".as_bytes(), 110)).0 {
            eprintln!("{:?}", v);
            panic!("found");
        }

        if let Some(v) = mt.get(&LookupKey::new("abf".as_bytes(), 110)).0 {
            eprintln!("{:?}", v);
            panic!("found");
        }

        // Bigger sequence number falls back to next smaller
        if let Some(v) = mt.get(&LookupKey::new("abc".as_bytes(), 116)).0 {
            assert_eq!(v, "122".as_bytes());
        } else {
            panic!("not found");
        }

        // Exact match works
        if let (Some(v), deleted) = mt.get(&LookupKey::new("abc".as_bytes(), 120)) {
            assert_eq!(v, "123".as_bytes());
            assert!(!deleted);
        } else {
            panic!("not found");
        }

        if let (None, deleted) = mt.get(&LookupKey::new("abe".as_bytes(), 122)) {
            assert!(deleted);
        } else {
            panic!("found deleted");
        }

        if let Some(v) = mt.get(&LookupKey::new("abf".as_bytes(), 129)).0 {
            assert_eq!(v, "126".as_bytes());
        } else {
            panic!("not found");
        }
    }

    #[test]
    fn test_memtable_iterator_init() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());
        iter.next();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
        );
        iter.reset();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_iterator_seek() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        assert!(!iter.valid());

        iter.seek(LookupKey::new("abc".as_bytes(), 400).internal_key());
        let (mut gotkey, gotval) = current_key_val(&iter).unwrap();
        truncate_to_userkey(&mut gotkey);
        assert_eq!(
            ("abc".as_bytes(), "123".as_bytes()),
            (gotkey.as_slice(), gotval.as_slice())
        );

        iter.seek(LookupKey::new("xxx".as_bytes(), 400).internal_key());
        assert!(!iter.valid());

        iter.seek(LookupKey::new("abd".as_bytes(), 400).internal_key());
        let (mut gotkey, gotval) = current_key_val(&iter).unwrap();
        truncate_to_userkey(&mut gotkey);
        assert_eq!(
            ("abd".as_bytes(), "124".as_bytes()),
            (gotkey.as_slice(), gotval.as_slice())
        );
    }

    #[test]
    fn test_memtable_iterator_fwd() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        let expected = [
            "123".as_bytes(), /* abc with higher sequence number comes first */
            "122".as_bytes(),
            "124".as_bytes(),
            // deleted entry:
            "125".as_bytes(),
            "126".as_bytes(),
        ];
        for (i, (k, v)) in LdbIteratorIter::wrap(&mut iter).enumerate() {
            assert_eq!(v, expected[i]);
        }
    }

    #[test]
    fn test_memtable_iterator_reverse() {
        let mt = get_memtable();
        let mut iter = mt.iter();

        // Bigger sequence number comes first
        iter.next();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
        );

        iter.next();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice()
        );

        iter.next();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 100, 1, 121, 0, 0, 0, 0, 0, 0].as_slice()
        );

        iter.prev();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 99, 1, 115, 0, 0, 0, 0, 0, 0].as_slice()
        );

        iter.prev();
        assert!(iter.valid());
        assert_eq!(
            current_key_val(&iter).unwrap().0,
            vec![97, 98, 99, 1, 120, 0, 0, 0, 0, 0, 0].as_slice()
        );

        iter.prev();
        assert!(!iter.valid());
    }

    #[test]
    fn test_memtable_parse_key() {
        let key = vec![11, 1, 2, 3, 1, 123, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6];
        let (keylen, keyoff, tag, vallen, valoff) = parse_memtable_key(&key);
        assert_eq!(keylen, 3);
        assert_eq!(&key[keyoff..keyoff + keylen], vec![1, 2, 3].as_slice());
        assert_eq!(tag, (123 << 8) | 1);
        assert_eq!(vallen, 3);
        assert_eq!(&key[valoff..valoff + vallen], vec![4, 5, 6].as_slice());
    }

    #[test]
    fn test_memtable_iterator_behavior() {
        let mut mt = MemTable::new(options::for_test().cmp);
        let entries = [
            (115, "abc", "122"),
            (120, "abd", "123"),
            (121, "abe", "124"),
            (123, "abf", "126"),
        ];

        for e in entries.iter() {
            mt.add(e.0, ValueType::TypeValue, e.1.as_bytes(), e.2.as_bytes());
        }

        test_iterator_properties(mt.iter());
    }
}
