use combycode_dpe::accumulators::*;

#[test]
fn number_basic() {
    let mut n = Number::new(0.0);
    assert_eq!(n.value(), 0.0);
    assert_eq!(n.inc(1.0), 1.0);
    assert_eq!(n.inc(5.0), 6.0);
    assert_eq!(n.dec(2.0), 4.0);
}

#[test]
fn average_basic() {
    let mut a = Average::new();
    assert_eq!(a.value(), 0.0);
    a.add(10.0);
    a.add(20.0);
    a.add(30.0);
    assert_eq!(a.value(), 20.0);
    assert_eq!(a.count(), 3);
}

#[test]
fn minmax_basic() {
    let mut mm = MinMax::new();
    assert!(mm.min().is_none());
    assert!(mm.max().is_none());
    for v in [3.0, 7.0, 1.0, 9.0, 5.0] {
        mm.add(v);
    }
    assert_eq!(mm.min(), Some(1.0));
    assert_eq!(mm.max(), Some(9.0));
    assert_eq!(mm.count(), 5);
}

#[test]
fn set_basic() {
    let mut s = Set::new();
    assert_eq!(s.size(), 0);
    s.add("a");
    s.add("b");
    s.add("a");
    assert_eq!(s.size(), 2);
    assert!(s.has("a"));
    assert!(!s.has("c"));
    s.delete("a");
    assert!(!s.has("a"));
}

#[test]
fn map_basic() {
    let mut m = Map::new();
    m.set("k", serde_json::json!("v"));
    assert!(m.has("k"));
    assert_eq!(m.get("k").unwrap(), &serde_json::json!("v"));
    m.delete("k");
    assert!(!m.has("k"));
    assert_eq!(m.size(), 0);
}

#[test]
fn buffer_basic() {
    let mut b = Buffer::new(Some(3));
    for i in 0..5 {
        b.push(serde_json::json!(i));
    }
    assert_eq!(b.size(), 3);
    let items: Vec<i64> = b.items().iter().map(|v| v.as_i64().unwrap()).collect();
    assert_eq!(items, vec![2, 3, 4]);
}

#[test]
fn buffer_unlimited() {
    let mut b = Buffer::new(None);
    for i in 0..100 {
        b.push(serde_json::json!(i));
    }
    assert_eq!(b.size(), 100);
    b.clear();
    assert_eq!(b.size(), 0);
}

#[test]
fn boolean_basic() {
    let mut b = Boolean::new(false);
    assert!(!b.value());
    b.toggle(None);
    assert!(b.value());
    b.toggle(Some(false));
    assert!(!b.value());
    b.set(true);
    assert!(b.value());
}

#[test]
fn bitmask_basic() {
    let mut bm = BitMask::new(&["a", "b", "c"]);
    assert!(!bm.any());
    assert!(!bm.all());
    bm.set("a").unwrap();
    assert!(bm.any());
    assert!(!bm.all());
    bm.set("b").unwrap();
    bm.set("c").unwrap();
    assert!(bm.all());
    bm.clear("a").unwrap();
    assert!(!bm.all());
    assert!(bm.any());
}

#[test]
fn bitmask_unknown_flag() {
    let mut bm = BitMask::new(&["a"]);
    assert!(bm.set("unknown").is_err());
    assert!(bm.get("unknown").is_err());
}

#[test]
fn memory_basic() {
    let mut mem = Memory::new();
    mem.number("count", 0.0).inc(5.0);
    assert_eq!(mem.number("count", 0.0).value(), 5.0);
    assert!(mem.has("count"));
    mem.delete("count");
    assert!(!mem.has("count"));
}

#[test]
fn memory_all_types() {
    let mut mem = Memory::new();
    mem.number("n", 0.0);
    mem.average("a");
    mem.minmax("mm");
    mem.set("s");
    mem.map("m");
    mem.buffer("b", None);
    mem.boolean("bool", false);
    mem.bitmask("bm", &["x"]);
    mem.trigger("t", |_| true);
    assert!(mem.check_trigger("t"));
}

#[test]
fn trigger_condition() {
    let mut mem = Memory::new();
    mem.number("count", 0.0);
    mem.number("expected", 0.0);
    mem.trigger("ready", |m| {
        let count = m.get_number("count").map(|n| n.value()).unwrap_or(0.0);
        let expected = m.get_number("expected").map(|n| n.value()).unwrap_or(0.0);
        expected > 0.0 && count >= expected
    });

    assert!(!mem.check_trigger("ready"));
    mem.number("expected", 0.0).inc(3.0);
    assert!(!mem.check_trigger("ready"));
    mem.number("count", 0.0).inc(1.0);
    mem.number("count", 0.0).inc(1.0);
    mem.number("count", 0.0).inc(1.0);
    assert!(mem.check_trigger("ready"));
}
