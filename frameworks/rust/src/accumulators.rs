//! Typed accumulators for shared memory across processors.

use std::collections::{HashMap, HashSet, VecDeque};
use serde_json::Value;

/// Numeric accumulator with increment/decrement.
pub struct Number {
    value: f64,
}

impl Number {
    pub fn new(initial: f64) -> Self {
        Self { value: initial }
    }

    pub fn inc(&mut self, delta: f64) -> f64 {
        self.value += delta;
        self.value
    }

    pub fn dec(&mut self, delta: f64) -> f64 {
        self.value -= delta;
        self.value
    }

    pub fn value(&self) -> f64 {
        self.value
    }
}

/// Running average without storing all values.
#[derive(Default)]
pub struct Average {
    sum: f64,
    count: u64,
}

impl Average {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }

    pub fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    pub fn value(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

/// Track minimum and maximum values.
#[derive(Default)]
pub struct MinMax {
    min: Option<f64>,
    max: Option<f64>,
    count: u64,
}

impl MinMax {
    pub fn new() -> Self {
        Self { min: None, max: None, count: 0 }
    }

    pub fn add(&mut self, value: f64) {
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
        self.count += 1;
    }

    pub fn min(&self) -> Option<f64> {
        self.min
    }

    pub fn max(&self) -> Option<f64> {
        self.max
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

/// Unique value collection.
#[derive(Default)]
pub struct Set {
    data: HashSet<String>,
}

impl Set {
    pub fn new() -> Self {
        Self { data: HashSet::new() }
    }

    pub fn add(&mut self, value: &str) {
        self.data.insert(value.to_string());
    }

    pub fn has(&self, value: &str) -> bool {
        self.data.contains(value)
    }

    pub fn delete(&mut self, value: &str) {
        self.data.remove(value);
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn values(&self) -> Vec<&String> {
        self.data.iter().collect()
    }
}

/// Key-value lookup. Transient storage.
#[derive(Default)]
pub struct Map {
    data: HashMap<String, Value>,
}

impl Map {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    pub fn set(&mut self, key: &str, value: Value) {
        self.data.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    pub fn has(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Ordered collection with optional size limit (sliding window).
pub struct Buffer {
    data: VecDeque<Value>,
    limit: Option<usize>,
}

impl Buffer {
    pub fn new(limit: Option<usize>) -> Self {
        Self {
            data: VecDeque::new(),
            limit,
        }
    }

    pub fn push(&mut self, item: Value) {
        if let Some(limit) = self.limit {
            if self.data.len() >= limit {
                self.data.pop_front();
            }
        }
        self.data.push_back(item);
    }

    pub fn items(&self) -> Vec<&Value> {
        self.data.iter().collect()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}

/// Simple flag.
pub struct Boolean {
    value: bool,
}

impl Boolean {
    pub fn new(initial: bool) -> Self {
        Self { value: initial }
    }

    pub fn set(&mut self, value: bool) {
        self.value = value;
    }

    pub fn toggle(&mut self, value: Option<bool>) {
        match value {
            Some(v) => self.value = v,
            None => self.value = !self.value,
        }
    }

    pub fn value(&self) -> bool {
        self.value
    }
}

/// Named flag set for tracking multiple boolean states.
pub struct BitMask {
    flags: HashMap<String, bool>,
}

impl BitMask {
    pub fn new(names: &[&str]) -> Self {
        let flags = names.iter().map(|n| (n.to_string(), false)).collect();
        Self { flags }
    }

    pub fn set(&mut self, name: &str) -> Result<(), String> {
        match self.flags.get_mut(name) {
            Some(v) => { *v = true; Ok(()) }
            None => Err(format!("Unknown flag: {}", name)),
        }
    }

    pub fn get(&self, name: &str) -> Result<bool, String> {
        self.flags.get(name).copied().ok_or_else(|| format!("Unknown flag: {}", name))
    }

    pub fn clear(&mut self, name: &str) -> Result<(), String> {
        match self.flags.get_mut(name) {
            Some(v) => { *v = false; Ok(()) }
            None => Err(format!("Unknown flag: {}", name)),
        }
    }

    pub fn all(&self) -> bool {
        self.flags.values().all(|&v| v)
    }

    pub fn any(&self) -> bool {
        self.flags.values().any(|&v| v)
    }
}

/// Condition checker. Condition is a closure over Memory.
pub struct Trigger {
    condition: Box<dyn Fn(&Memory) -> bool>,
}

impl Trigger {
    pub fn new(condition: impl Fn(&Memory) -> bool + 'static) -> Self {
        Self { condition: Box::new(condition) }
    }

    pub fn check(&self, memory: &Memory) -> bool {
        (self.condition)(memory)
    }
}

/// Accumulator enum for type-safe storage in Memory.
pub enum Accumulator {
    Number(Number),
    Average(Average),
    MinMax(MinMax),
    Set(Set),
    Map(Map),
    Buffer(Buffer),
    Boolean(Boolean),
    BitMask(BitMask),
    Trigger(Trigger),
}

/// Shared memory — typed accumulator registry.
#[derive(Default)]
pub struct Memory {
    store: HashMap<String, Accumulator>,
}

impl Memory {
    pub fn new() -> Self {
        Self { store: HashMap::new() }
    }

    pub fn number(&mut self, name: &str, initial: f64) -> &mut Number {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Number(Number::new(initial)));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Number(n) => n,
            _ => panic!("Accumulator '{}' is not a Number", name),
        }
    }

    pub fn average(&mut self, name: &str) -> &mut Average {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Average(Average::new()));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Average(a) => a,
            _ => panic!("Accumulator '{}' is not an Average", name),
        }
    }

    pub fn minmax(&mut self, name: &str) -> &mut MinMax {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::MinMax(MinMax::new()));
        match self.store.get_mut(name).unwrap() {
            Accumulator::MinMax(m) => m,
            _ => panic!("Accumulator '{}' is not a MinMax", name),
        }
    }

    pub fn set(&mut self, name: &str) -> &mut Set {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Set(Set::new()));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Set(s) => s,
            _ => panic!("Accumulator '{}' is not a Set", name),
        }
    }

    pub fn map(&mut self, name: &str) -> &mut Map {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Map(Map::new()));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Map(m) => m,
            _ => panic!("Accumulator '{}' is not a Map", name),
        }
    }

    pub fn buffer(&mut self, name: &str, limit: Option<usize>) -> &mut Buffer {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Buffer(Buffer::new(limit)));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Buffer(b) => b,
            _ => panic!("Accumulator '{}' is not a Buffer", name),
        }
    }

    pub fn boolean(&mut self, name: &str, initial: bool) -> &mut Boolean {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Boolean(Boolean::new(initial)));
        match self.store.get_mut(name).unwrap() {
            Accumulator::Boolean(b) => b,
            _ => panic!("Accumulator '{}' is not a Boolean", name),
        }
    }

    pub fn bitmask(&mut self, name: &str, flags: &[&str]) -> &mut BitMask {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::BitMask(BitMask::new(flags)));
        match self.store.get_mut(name).unwrap() {
            Accumulator::BitMask(b) => b,
            _ => panic!("Accumulator '{}' is not a BitMask", name),
        }
    }

    pub fn trigger(&mut self, name: &str, condition: impl Fn(&Memory) -> bool + 'static) {
        self.store.entry(name.to_string())
            .or_insert_with(|| Accumulator::Trigger(Trigger::new(condition)));
    }

    pub fn check_trigger(&self, name: &str) -> bool {
        match self.store.get(name) {
            Some(Accumulator::Trigger(t)) => t.check(self),
            _ => false,
        }
    }

    pub fn has(&self, name: &str) -> bool {
        self.store.contains_key(name)
    }

    pub fn delete(&mut self, name: &str) {
        self.store.remove(name);
    }

    // -- Immutable getters for reading accumulator values (used in trigger closures) --

    pub fn get_number(&self, name: &str) -> Option<&Number> {
        match self.store.get(name) {
            Some(Accumulator::Number(n)) => Some(n),
            _ => None,
        }
    }

    pub fn get_average(&self, name: &str) -> Option<&Average> {
        match self.store.get(name) {
            Some(Accumulator::Average(a)) => Some(a),
            _ => None,
        }
    }

    pub fn get_minmax(&self, name: &str) -> Option<&MinMax> {
        match self.store.get(name) {
            Some(Accumulator::MinMax(m)) => Some(m),
            _ => None,
        }
    }

    pub fn get_set(&self, name: &str) -> Option<&Set> {
        match self.store.get(name) {
            Some(Accumulator::Set(s)) => Some(s),
            _ => None,
        }
    }

    pub fn get_map(&self, name: &str) -> Option<&Map> {
        match self.store.get(name) {
            Some(Accumulator::Map(m)) => Some(m),
            _ => None,
        }
    }

    pub fn get_boolean(&self, name: &str) -> Option<&Boolean> {
        match self.store.get(name) {
            Some(Accumulator::Boolean(b)) => Some(b),
            _ => None,
        }
    }

    pub fn get_bitmask(&self, name: &str) -> Option<&BitMask> {
        match self.store.get(name) {
            Some(Accumulator::BitMask(b)) => Some(b),
            _ => None,
        }
    }
}
