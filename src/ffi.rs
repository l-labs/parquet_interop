//! ffi — the sliver of the L host ABI this adapter uses, nothing more.
//!
//! A `K` is a TAGGED 64-bit VALUE, not a pointer.  The top 6 bits hold a
//! type tag; the low 58 bits hold either an inline atom value or a heap
//! payload pointer.  vtag(x) != 0 means atom; vtag(x) == 0 means a heap
//! object (vector/list/dict/table) whose 32-byte header sits at NEGATIVE
//! offsets from the payload base: subtype at base-28, element count at
//! base-24.  These facts mirror the host exactly — do not "improve" them.
//!
//! None of the extern symbols below are linked at build time: this cdylib
//! is dlopen'd into the L binary by `2:`, so ktn/sn/... bind against the
//! host process at load time (see build.rs for the macOS link flag).

use std::os::raw::c_char;

/// The tagged 64-bit K value.
pub type K = u64;

// ── Type tags — atoms report the POSITIVE tag in vtag(); heap ──────────
// vectors report tag 0 with the element type in vt().
pub const KB: i16 = 1;                                                          // boolean (byte per value)
pub const KG: i16 = 4;                                                          // byte
pub const KH: i16 = 5;                                                          // short (i16)
pub const KI: i16 = 6;                                                          // int (i32) — L's default int
pub const KJ: i16 = 7;                                                          // long (i64)
pub const KE: i16 = 8;                                                          // real (f32)
pub const KF: i16 = 9;                                                          // float (f64)
pub const KS: i16 = 11;                                                         // symbol (interned char*)
pub const KP: i16 = 12;                                                         // timestamp (ns since 2000)
pub const KD: i16 = 14;                                                         // date (days since 2000)
pub const KZ: i16 = 15;                                                         // datetime (f64 days since 2000)
pub const KN: i16 = 16;                                                         // timespan (ns, no epoch)
pub const KT: i16 = 19;                                                         // time (ms since midnight)
pub const XT: i16 = 98;                                                         // table (flip of a column dict)

// ── Null sentinels ──────────────────────────────────────────────────────
pub const NI: i32 = i32::MIN;                                                   // 0Ni — int/date/time null
pub const NJ: i64 = i64::MIN;                                                   // 0Nj — long/timestamp/timespan null
pub const NH: i16 = i16::MIN;                                                   // 0Nh — short null

// ── Epoch shifts: Arrow/Parquet epoch is 1970, L epoch is 2000 ─────────
pub const DAY2000: i32 = 10_957;                                                // days 1970-01-01 → 2000-01-01
pub const NS2000: i64 = 946_684_800_000_000_000;                                // ns between the epochs

// ── Tagged-value decoding (host macros vtag/va/vt/vn/kt/kn) ────────────
const TAG_SHIFT: u32 = 58;                                                      // tag lives in the top 6 bits
const PTR_MASK: u64 = !0u64 >> 6;                                               // low 58 bits = value / pointer

/// Type tag of x: nonzero → atom, zero → heap object.
#[inline]
pub fn vtag(x: K) -> i16 {
    (x >> TAG_SHIFT) as i16
}

/// Payload base pointer (byte 0 of vector data / interned symbol chars).
#[inline]
pub fn va(x: K) -> *mut u8 {
    (x & PTR_MASK) as *mut u8
}

/// Heap subtype (only valid when vtag(x) == 0): header word at base-28.
#[inline]
pub unsafe fn vt(x: K) -> i16 {
    *(va(x).offset(-28) as *const i16)
}

/// Heap element count (only valid when vtag(x) == 0): word at base-24.
#[inline]
pub unsafe fn vn(x: K) -> i64 {
    *(va(x).offset(-24) as *const i64)
}

/// Legacy SIGNED type: atom → -tag, heap → subtype.  Ported extensions
/// keep their `kt(x) == -KS` / `== KS` checks unchanged this way.
#[inline]
pub unsafe fn kt(x: K) -> i16 {
    let t = vtag(x);
    if t != 0 { -t } else { vt(x) }
}

/// Element count with the atom-counts-as-1 convention.
#[inline]
pub unsafe fn kn(x: K) -> i64 {
    if vtag(x) != 0 { 1 } else { vn(x) }
}

/// Symbol atom → interned NUL-terminated char* (host `ls`).
#[inline]
pub unsafe fn ls(x: K) -> *const c_char {
    va(x) as *const c_char
}

// ── Typed payload views (host vG/vI/vJ/... macros) ─────────────────────
#[inline]
pub fn v_g(x: K) -> *mut u8 {
    va(x)
}
#[inline]
pub fn v_h(x: K) -> *mut i16 {
    va(x) as *mut i16
}
#[inline]
pub fn v_i(x: K) -> *mut i32 {
    va(x) as *mut i32
}
#[inline]
pub fn v_j(x: K) -> *mut i64 {
    va(x) as *mut i64
}
#[inline]
pub fn v_e(x: K) -> *mut f32 {
    va(x) as *mut f32
}
#[inline]
pub fn v_f(x: K) -> *mut f64 {
    va(x) as *mut f64
}
#[inline]
pub fn v_s(x: K) -> *mut *mut c_char {
    va(x) as *mut *mut c_char
}
#[inline]
pub fn v_k(x: K) -> *mut K {
    va(x) as *mut K
}

// ── Host functions (resolved from the L process at dlopen time) ────────
extern "C" {
    pub fn ktn(t: i32, n: i32) -> K;                                            // typed vector of n elements
    pub fn kj(x: i64) -> K;                                                     // long atom constructor
    pub fn xD(keys: K, vals: K) -> K;                                           // dict from keys + values
    pub fn xT(dict: K) -> K;                                                    // table from a column dict
    pub fn sn(s: *const c_char, n: i32) -> *mut c_char;                         // intern n bytes
    pub fn nt(t: u32) -> i64;                                                   // storage byte width by type tag
    pub fn r1(x: K) -> K;                                                       // retain (refcount++)
    pub fn r0(x: K);                                                            // release (refcount--/free)
    pub fn krr(msg: *const c_char) -> K;                                        // raise an error from a string
}

/// Raise an L error carrying `msg`.  krr keeps the pointer without
/// copying, so the CString is deliberately LEAKED: the error path is
/// rare and messages are tiny — a leak is the only way to guarantee the
/// bytes outlive the raise no matter when L formats the message.
pub fn err(msg: &str) -> K {
    let clean: String = msg.chars().filter(|c| *c != '\0').collect();
    let c = std::ffi::CString::new(clean).unwrap();
    unsafe { krr(c.into_raw()) }
}

/// Intern `bytes` (need not be NUL-terminated) → permanent symbol ptr.
#[inline]
pub unsafe fn intern(bytes: &[u8]) -> *mut c_char {
    sn(bytes.as_ptr() as *const c_char, bytes.len() as i32)
}
