#![allow(dead_code)]

pub fn key_to_hex(key: &[u8]) -> Vec<u8> {
    let mut nibbles = Vec::new();
    for k in key {
        nibbles.push(k / 16);
        nibbles.push(k % 16);
    }
    nibbles
}

pub fn hex_to_compact(s: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut first_byte = 1 << 5;
    let mut i = 0;
    if s.len() & 1 == 1 {
        first_byte |= 1 << 4;
        first_byte |= s[0];
        i = 1;
    }
    buf.push(first_byte);
    while i < s.len() {
        buf.push(s[i] << 4 | s[i + 1]);
        i += 2;
    }

    assert!(buf.len() == s.len() / 2 + 1);
    buf
}

pub fn compact_to_hex(c: &[u8]) -> Vec<u8> {
    if c.len() == 0 {
        return c.to_vec();
    }
    let base = key_to_hex(c);
    let mut end = base.len();
    if base[0] < 2 {
        end -= 1;
    }
    let start = 2 - (base[0] & 1);
    base[start as usize..end].to_vec()
}

pub fn to_nibbles(bytes: &[u8]) -> impl Iterator<Item = u8> + '_ {
    bytes
        .iter()
        .flat_map(|b| [(b >> 4) & 0xf, b & 0xf].into_iter())
}

pub fn from_nibbles(nibbles: &[u8]) -> impl Iterator<Item = u8> + '_ {
    assert!(nibbles.len() & 1 == 0);
    nibbles.chunks_exact(2).map(|p| (p[0] << 4) | p[1])
}

pub fn to_path(key: &[u8]) -> Vec<u8> {
    let mut path: Vec<u8> = to_nibbles(key).collect();
    path.push(16);
    path
}

pub fn to_compact(path: &[u8]) -> Vec<u8> {
    let terminator: u8 = (path.len() > 0 && path[path.len() - 1] == 16) as u8;
    let len = path.len() - terminator as usize;
    let mut res = if len & 1 == 1 {
        vec![(terminator << 1) + 1]
    } else {
        vec![terminator << 1, 0 as u8]
    };
    res.extend(&path[..len]);
    from_nibbles(&res).collect()
}

pub fn from_compact(compact: &[u8]) -> Vec<u8> {
    let mut nibbles: Vec<u8> = to_nibbles(&compact).collect();
    if nibbles[0] >= 2 {
        nibbles.push(16)
    }
    let head = 2 - (nibbles[0] & 1) as usize;
    nibbles[head..].to_vec()
}
