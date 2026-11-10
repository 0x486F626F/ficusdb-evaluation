//! Hand-written storage codec that produces exactly the bytes of the `rlp`-crate encoding
//! in node.rs (`rlp::encode(&NodeType)`), without its per-item overhead. That format
//! stores every byte array (value, extra, hash, path) as an RLP *list of u8 integers*,
//! so the generic encoder handled them one byte at a time.
//!
//! Rules (rlp 0.6.1): an integer is its minimal big-endian bytes as a string, 0 -> 0x80,
//! one byte < 0x80 is its own encoding; a string/list payload of <= 55 bytes gets a 1-byte
//! header (0x80+len / 0xc0+len), longer ones 0xb7/0xf7 + big-endian length;
//! Option: None -> empty list, Some(x) -> list [x].
//!
//! `decode` takes a fast path only for canonical input and otherwise returns None, so the
//! caller falls back to the `rlp` crate (same accept/reject behaviour as before).

use super::node::{Branch, Child, NodePtr, NodeType, Short, Value};
use super::{CleanPtr, NBRANCH};

const BRANCH: u8 = 0x0;
const SHORT: u8 = 0x1;
const VALUE: u8 = 0x2;

// ---------------- sizes ----------------
#[inline]
fn u8_item_len(b: u8) -> usize {
    if b < 0x80 { 1 } else { 2 } // 0 -> 0x80 (1 byte)
}
#[inline]
fn uint_len(x: u64) -> usize {
    if x < 0x80 { 1 } else { 1 + (8 - x.leading_zeros() as usize / 8) }
}
#[inline]
fn hdr_len(payload: usize) -> usize {
    if payload <= 55 { 1 } else { 1 + (8 - (payload as u64).leading_zeros() as usize / 8) }
}
#[inline]
fn bytes_list_payload(v: &[u8]) -> usize {
    v.iter().map(|&b| u8_item_len(b)).sum()
}
#[inline]
fn bytes_list_len(v: &[u8]) -> usize {
    let p = bytes_list_payload(v);
    hdr_len(p) + p
}
#[inline]
fn child_len(c: &Option<Child>) -> usize {
    match c {
        None => 1,
        Some(Child::Ptr(NodePtr::Clean(p))) | Some(Child::Hash(p, _)) => {
            let p = uint_len(*p);
            hdr_len(p) + p
        }
        Some(Child::Ptr(NodePtr::Dirty(_))) => unreachable!("encoding a dirty child"),
    }
}

// ---------------- writers ----------------
#[inline]
fn put_list_hdr(out: &mut Vec<u8>, payload: usize) {
    if payload <= 55 {
        out.push(0xc0 + payload as u8);
    } else {
        let be = (payload as u64).to_be_bytes();
        let skip = (payload as u64).leading_zeros() as usize / 8;
        out.push(0xf7 + (8 - skip) as u8);
        out.extend_from_slice(&be[skip..]);
    }
}
#[inline]
fn put_uint(out: &mut Vec<u8>, x: u64) {
    if x == 0 {
        out.push(0x80);
    } else if x < 0x80 {
        out.push(x as u8);
    } else {
        let be = x.to_be_bytes();
        let skip = x.leading_zeros() as usize / 8;
        out.push(0x80 + (8 - skip) as u8);
        out.extend_from_slice(&be[skip..]);
    }
}
#[inline]
fn put_bytes_list(out: &mut Vec<u8>, v: &[u8]) {
    put_list_hdr(out, bytes_list_payload(v));
    for &b in v {
        if b == 0 {
            out.push(0x80);
        } else if b < 0x80 {
            out.push(b);
        } else {
            out.push(0x81);
            out.push(b);
        }
    }
}
fn put_child(out: &mut Vec<u8>, c: &Option<Child>) {
    match c {
        None => out.push(0xc0),
        Some(Child::Ptr(NodePtr::Clean(p))) | Some(Child::Hash(p, _)) => {
            put_list_hdr(out, uint_len(*p));
            put_uint(out, *p);
        }
        Some(Child::Ptr(NodePtr::Dirty(_))) => unreachable!("encoding a dirty child"),
    }
}

fn body_payload(n: &NodeType) -> usize {
    match n {
        NodeType::Branch(b) => {
            let ch: usize = b.children.iter().map(child_len).sum();
            bytes_list_len(&b.hash) + hdr_len(ch) + ch + uint_len(b.aha_len as u64) + uint_len(b.aha_ptr)
        }
        NodeType::Short(s) => {
            let c = match &s.child {
                Child::Ptr(NodePtr::Clean(p)) | Child::Hash(p, _) => uint_len(*p),
                Child::Ptr(NodePtr::Dirty(_)) => unreachable!("encoding a dirty child"),
            };
            bytes_list_len(&s.hash) + bytes_list_len(&s.path) + c
        }
        NodeType::Value(v) => bytes_list_len(&v.value) + bytes_list_len(&v.extra),
    }
}

/// Exact encoded length of `rlp::encode(n)` (used by the codec tests).
#[cfg(test)]
pub fn encoded_len(n: &NodeType) -> usize {
    let body = body_payload(n);
    let outer = uint_len(type_of(n) as u64) + hdr_len(body) + body;
    hdr_len(outer) + outer
}

fn type_of(n: &NodeType) -> u8 {
    match n {
        NodeType::Branch(_) => BRANCH,
        NodeType::Short(_) => SHORT,
        NodeType::Value(_) => VALUE,
    }
}

/// Append exactly `rlp::encode(n)` to `out`.
pub fn encode_into(n: &NodeType, out: &mut Vec<u8>) {
    let body = body_payload(n);
    let outer = uint_len(type_of(n) as u64) + hdr_len(body) + body;
    out.reserve(hdr_len(outer) + outer);
    put_list_hdr(out, outer);
    put_uint(out, type_of(n) as u64);
    put_list_hdr(out, body);
    match n {
        NodeType::Branch(b) => {
            put_bytes_list(out, &b.hash);
            let ch: usize = b.children.iter().map(child_len).sum();
            put_list_hdr(out, ch);
            for c in b.children.iter() {
                put_child(out, c);
            }
            put_uint(out, b.aha_len as u64);
            put_uint(out, b.aha_ptr);
        }
        NodeType::Short(s) => {
            put_bytes_list(out, &s.hash);
            put_bytes_list(out, &s.path);
            match &s.child {
                Child::Ptr(NodePtr::Clean(p)) | Child::Hash(p, _) => put_uint(out, *p),
                Child::Ptr(NodePtr::Dirty(_)) => unreachable!("encoding a dirty child"),
            }
        }
        NodeType::Value(v) => {
            put_bytes_list(out, &v.value);
            put_bytes_list(out, &v.extra);
        }
    }
}

// ---------------- fast canonical decoder ----------------
struct R<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> R<'a> {
    /// List header -> payload end offset. Canonical only.
    fn list(&mut self) -> Option<usize> {
        let h = *self.d.get(self.i)?;
        self.i += 1;
        let len = if (0xc0..=0xf7).contains(&h) {
            (h - 0xc0) as usize
        } else if h > 0xf7 {
            let ll = (h - 0xf7) as usize;
            if ll > 8 || self.i + ll > self.d.len() || self.d[self.i] == 0 {
                return None;
            }
            let mut len = 0usize;
            for k in 0..ll {
                len = (len << 8) | self.d[self.i + k] as usize;
            }
            self.i += ll;
            if len <= 55 {
                return None;
            }
            len
        } else {
            return None;
        };
        let end = self.i.checked_add(len)?;
        if end > self.d.len() {
            return None;
        }
        Some(end)
    }
    fn uint(&mut self, max_bytes: usize) -> Option<u64> {
        let h = *self.d.get(self.i)?;
        self.i += 1;
        if h == 0 {
            return None; // 0 must be 0x80
        }
        if h < 0x80 {
            return Some(h as u64);
        }
        if h == 0x80 {
            return Some(0);
        }
        if h > 0xb7 {
            return None;
        }
        let n = (h - 0x80) as usize;
        if n > max_bytes || self.i + n > self.d.len() || self.d[self.i] == 0 {
            return None;
        }
        if n == 1 && self.d[self.i] < 0x80 {
            return None;
        }
        let mut x = 0u64;
        for k in 0..n {
            x = (x << 8) | self.d[self.i + k] as u64;
        }
        self.i += n;
        Some(x)
    }
    fn bytes_list(&mut self) -> Option<Vec<u8>> {
        let end = self.list()?;
        let mut v = Vec::with_capacity(end - self.i);
        while self.i < end {
            let b = self.d[self.i];
            if b == 0x80 {
                v.push(0);
                self.i += 1;
            } else if b != 0 && b < 0x80 {
                v.push(b);
                self.i += 1;
            } else if b == 0x81 && self.i + 1 < end && self.d[self.i + 1] >= 0x80 {
                v.push(self.d[self.i + 1]);
                self.i += 2;
            } else {
                return None;
            }
        }
        Some(v)
    }
    fn child(&mut self) -> Option<Option<Child>> {
        let end = self.list()?;
        if self.i == end {
            return Some(None);
        }
        let p: CleanPtr = self.uint(8)?;
        if self.i != end {
            return None;
        }
        Some(Some(Child::Ptr(NodePtr::Clean(p))))
    }
}

/// Decode a canonical encoding produced by `encode_into`; None means "use the rlp crate".
pub fn decode(data: &[u8]) -> Option<NodeType> {
    let mut r = R { d: data, i: 0 };
    let outer = r.list()?;
    if outer != data.len() {
        return None;
    }
    let t = r.uint(1)? as u8;
    let body = r.list()?;
    let n = match t {
        BRANCH => {
            let hash = r.bytes_list()?;
            let ch_end = r.list()?;
            let mut children: [Option<Child>; NBRANCH + 1] = std::array::from_fn(|_| None);
            let mut k = 0;
            while r.i < ch_end {
                if k > NBRANCH {
                    return None;
                }
                children[k] = r.child()?;
                k += 1;
            }
            if k != NBRANCH + 1 {
                return None; // the rlp path handles other lengths (unwrap_or_default)
            }
            let aha_len = r.uint(1)? as u8;
            let aha_ptr = r.uint(8)?;
            NodeType::Branch(Box::new(Branch { hash, children, aha_len, aha_ptr }))
        }
        SHORT => {
            let hash = r.bytes_list()?;
            let path = r.bytes_list()?;
            let p = r.uint(8)?;
            NodeType::Short(Short { hash, path, child: Child::Ptr(NodePtr::Clean(p)) })
        }
        VALUE => {
            let value = r.bytes_list()?;
            let extra = r.bytes_list()?;
            NodeType::Value(Value { value, extra })
        }
        _ => return None,
    };
    if r.i != body || body != outer {
        return None;
    }
    Some(n)
}

// ---------------- canonical trie RLP (the bytes that get hashed) ----------------
// Byte-identical to the RlpStream code in node.rs: Branch = list of 17 items (empty child
// -> 0x80, loaded child -> its reference item raw); Short = [compact path as a byte
// string, child reference raw]; a byte string uses the standard RLP string rules.

#[inline]
fn str_hdr_len(v: &[u8]) -> usize {
    if v.len() == 1 && v[0] < 0x80 { 0 } else { hdr_len(v.len()) }
}

/// Append `v` as an RLP byte string.
#[inline]
pub fn put_rlp_bytes(out: &mut Vec<u8>, v: &[u8]) {
    if v.len() == 1 && v[0] < 0x80 {
        out.push(v[0]);
    } else if v.len() <= 55 {
        out.push(0x80 + v.len() as u8);
        out.extend_from_slice(v);
    } else {
        let n = v.len() as u64;
        let skip = n.leading_zeros() as usize / 8;
        out.push(0xb7 + (8 - skip) as u8);
        out.extend_from_slice(&n.to_be_bytes()[skip..]);
        out.extend_from_slice(v);
    }
}

/// `rlp::encode(&v)` for a byte slice.
pub fn rlp_bytes(v: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(str_hdr_len(v) + v.len());
    put_rlp_bytes(&mut out, v);
    out
}

/// Canonical branch RLP; None if a child hash is not loaded.
pub fn branch_rlp(children: &[Option<Child>; NBRANCH + 1]) -> Option<Vec<u8>> {
    let mut payload = 0;
    for c in children.iter() {
        payload += match c {
            None => 1,
            Some(Child::Hash(_, h)) => h.len(),
            _ => return None,
        };
    }
    let mut out = Vec::with_capacity(hdr_len(payload) + payload);
    put_list_hdr(&mut out, payload);
    for c in children.iter() {
        match c {
            None => out.push(0x80),
            Some(Child::Hash(_, h)) => out.extend_from_slice(h),
            _ => unreachable!(),
        }
    }
    Some(out)
}

/// Canonical short-node RLP from its compact path and its child's reference item.
pub fn short_rlp(compact: &[u8], child_ref: &[u8]) -> Vec<u8> {
    let payload = str_hdr_len(compact) + compact.len() + child_ref.len();
    let mut out = Vec::with_capacity(hdr_len(payload) + payload);
    put_list_hdr(&mut out, payload);
    put_rlp_bytes(&mut out, compact);
    out.extend_from_slice(child_ref);
    out
}
