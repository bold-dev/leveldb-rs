use std::collections::{BTreeMap, HashMap};

pub type CacheKey = [u8; 16];
pub type CacheID = u64;

/// Safe LRU cache backed by a BTreeMap (for eviction order) and a HashMap (for key lookup).
/// Replaces the previous unsafe raw-pointer LRU linked list implementation.
pub struct Cache<T> {
    map: HashMap<CacheKey, (T, u64)>,
    order: BTreeMap<u64, CacheKey>,
    cap: usize,
    id: u64,
    tick: u64,
}

impl<T> Cache<T> {
    pub fn new(capacity: usize) -> Cache<T> {
        assert!(capacity > 0);
        Cache {
            map: HashMap::with_capacity(1024),
            order: BTreeMap::new(),
            cap: capacity,
            id: 0,
            tick: 0,
        }
    }

    pub fn new_cache_id(&mut self) -> CacheID {
        self.id += 1;
        self.id
    }

    pub fn count(&self) -> usize {
        self.map.len()
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn insert(&mut self, key: &CacheKey, elem: T) {
        // If key already exists, remove old entry from order map.
        if let Some((_, old_tick)) = self.map.remove(key) {
            self.order.remove(&old_tick);
        }

        // Evict oldest if at capacity.
        if self.map.len() >= self.cap {
            if let Some((&oldest_tick, _)) = self.order.iter().next() {
                let oldest_key = self.order.remove(&oldest_tick).unwrap();
                self.map.remove(&oldest_key);
            }
        }

        self.tick += 1;
        self.map.insert(*key, (elem, self.tick));
        self.order.insert(self.tick, *key);
    }

    pub fn get<'a>(&'a mut self, key: &CacheKey) -> Option<&'a T> {
        // Promote to newest on access.
        if let Some((_, old_tick)) = self.map.get(key) {
            let old_tick = *old_tick;
            self.order.remove(&old_tick);
            self.tick += 1;
            self.order.insert(self.tick, *key);
            self.map.get_mut(key).unwrap().1 = self.tick;
            Some(&self.map.get(key).unwrap().0)
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: &CacheKey) -> Option<T> {
        if let Some((elem, tick)) = self.map.remove(key) {
            self.order.remove(&tick);
            Some(elem)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(a: u8, b: u8, c: u8) -> CacheKey {
        [a, b, c, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    #[test]
    fn test_blockcache_cache_add_rm() {
        let mut cache = Cache::new(128);

        let h_123 = make_key(1, 2, 3);
        let h_521 = make_key(1, 2, 4);
        let h_372 = make_key(3, 4, 5);
        let h_332 = make_key(6, 3, 1);
        let h_899 = make_key(8, 2, 1);

        cache.insert(&h_123, 123);
        cache.insert(&h_332, 332);
        cache.insert(&h_521, 521);
        cache.insert(&h_372, 372);
        cache.insert(&h_899, 899);

        assert_eq!(cache.count(), 5);

        assert_eq!(cache.get(&h_123), Some(&123));
        assert_eq!(cache.get(&h_372), Some(&372));

        assert_eq!(cache.remove(&h_521), Some(521));
        assert_eq!(cache.get(&h_521), None);
        assert_eq!(cache.remove(&h_521), None);

        assert_eq!(cache.count(), 4);
    }

    #[test]
    fn test_blockcache_cache_capacity() {
        let mut cache = Cache::new(3);

        let h_123 = make_key(1, 2, 3);
        let h_521 = make_key(1, 2, 4);
        let h_372 = make_key(3, 4, 5);
        let h_332 = make_key(6, 3, 1);
        let h_899 = make_key(8, 2, 1);

        cache.insert(&h_123, 123);
        cache.insert(&h_332, 332);
        cache.insert(&h_521, 521);
        cache.insert(&h_372, 372);
        cache.insert(&h_899, 899);

        assert_eq!(cache.count(), 3);

        assert_eq!(cache.get(&h_123), None);
        assert_eq!(cache.get(&h_332), None);
        assert_eq!(cache.get(&h_521), Some(&521));
        assert_eq!(cache.get(&h_372), Some(&372));
        assert_eq!(cache.get(&h_899), Some(&899));
    }

    #[test]
    fn test_blockcache_lru_eviction_order() {
        let mut cache = Cache::new(3);

        let k1 = make_key(1, 0, 0);
        let k2 = make_key(2, 0, 0);
        let k3 = make_key(3, 0, 0);
        let k4 = make_key(4, 0, 0);

        cache.insert(&k1, 1);
        cache.insert(&k2, 2);
        cache.insert(&k3, 3);

        // Access k1 to promote it.
        assert_eq!(cache.get(&k1), Some(&1));

        // Insert k4 — should evict k2 (oldest after k1 was promoted).
        cache.insert(&k4, 4);
        assert_eq!(cache.count(), 3);
        assert_eq!(cache.get(&k2), None);
        assert_eq!(cache.get(&k1), Some(&1));
        assert_eq!(cache.get(&k3), Some(&3));
        assert_eq!(cache.get(&k4), Some(&4));
    }
}
