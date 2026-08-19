//! write — L table → Parquet file.
//!
//! The file is produced by a work queue over `row group × column`
//! chunks: every task slices its own row range out of the L column,
//! builds the Arrow array, encodes and compresses it into an
//! `ArrowColumnChunk`, and the master thread appends the finished
//! chunks to `SerializedFileWriter` in (row group, column) order.  The
//! bytes are therefore identical to what a serial writer would emit
//! for the same properties — only the wall time differs.  Nothing is
//! materialized for the whole table: peak transient memory is the
//! bounded set of in-flight chunks (see `window`), not a second copy
//! of the data.
//!
//! Performance model.  Numeric columns are handed to the encoder
//! WITHOUT A COPY: `Buffer::from_custom_allocation` wraps the K payload
//! itself, so the only thing allocated per column chunk is the null
//! bitmap (one sentinel pass, dropped entirely when no sentinel
//! occurs).  The exceptions are the two epoch-shifted types (KD/KP),
//! whose transformed values must be materialized — one pass, one
//! buffer, bitmap included — and the three types whose L and Arrow
//! layouts genuinely differ: KB (byte per value vs bit), KS (interned
//! pointers vs dictionary keys) and KZ (f64 days vs i64 ns).  Symbol
//! columns never touch the string bytes per row: L symbols are
//! INTERNED, so a pointer-keyed (integer-hashed) map turns the slice
//! into `DictionaryArray` keys, and each DISTINCT pointer is
//! `strlen`'d and utf8-validated exactly once.
//!
//! Encoding policy (owner decision: standard Parquet only, readable by
//! pyarrow >= 15 / duckdb / polars / Spark):
//!   * compression defaults to UNCOMPRESSED: the light encodings below
//!     already take most of what a block codec would have found, and
//!     what is left is a storage question rather than a CPU one — the
//!     write is bound by bytes-reaching-the-disk, so on a slow volume
//!     a codec can be FASTER than none (measured on the box: zstd-3
//!     writes 1.45 GB in 1.67 s where none writes 2.60 GB in 2.31 s);
//!   * symbols → dictionary + RLE_DICTIONARY (arrow-rs falls back to
//!     PLAIN by itself once the dictionary outgrows its page limit);
//!   * integral and temporal columns → sampled rule, see `pick_int`:
//!     sorted → DELTA_BINARY_PACKED, low cardinality → dictionary,
//!     otherwise PLAIN;
//!   * floats → PLAIN (BYTE_STREAM_SPLIT was measured and rejected:
//!     see `pick`);
//!   * booleans → RLE;
//!   * statistics → chunk-level min/max/null_count.
//!
//! Null policy mirrors arrow_interop's writer: 0Ni/0Nj/NaN(f64) become
//! Parquet nulls; short/byte/real/bool/symbol columns are written fully
//! valid — the empty symbol round-trips as the empty string, NOT as
//! null.  KH is ASYMMETRIC on purpose: 0Nh goes out as its bit pattern
//! (-32768), visible to a foreign reader and in the chunk statistics,
//! yet a Parquet Int16 null still reads back AS 0Nh.

use crate::ffi::*;
use crate::{val_at, Ctx, Val};
use arrow::array::*;
use arrow::buffer::{
    BooleanBuffer, Buffer, NullBuffer, ScalarBuffer,
};
use arrow::datatypes::*;
use arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use parquet::arrow::arrow_writer::{
    compute_leaves, get_column_writers, ArrowColumnChunk, ArrowColumnWriter,
};
use parquet::arrow::{ArrowSchemaConverter, ARROW_SCHEMA_META_KEY};
use parquet::basic::{Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::CStr;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::BufWriter;
use std::os::raw::c_char;
use std::ptr::NonNull;
use std::sync::{Arc, Condvar, Mutex};

/// Default rows per Parquet row group: big enough for good compression
/// and column-chunk locality, small enough to bound reader memory.
const ROW_GROUP: usize = 1 << 20;

/// Values probed per column when choosing an integral encoding.  The
/// probe is a STRIDE of `n / SAMPLE`, so it costs at most this many
/// cache misses — a millisecond against a multi-second write.  A
/// stride, not a spread: it starts at row 0, so it stops within one
/// stride of the end, and for `SAMPLE <= n < 2*SAMPLE` the stride is 1
/// and the probe is the first `SAMPLE` rows only.
const SAMPLE: usize = 8192;

/// Cardinality below which a dictionary beats the alternatives: the
/// indices pack into 16 bits or fewer, and the dictionary page itself
/// (2^16 * 8 bytes = 512 KB) still fits arrow-rs's 1 MB page limit, so
/// the encoder never has to fall back mid-chunk.
const DICT_CARD: usize = 1 << 16;

/// Bytes the master batches before one write(2).  arrow-rs splices a
/// finished chunk with `std::io::copy` into its own 8 KiB `BufWriter`,
/// which without this would turn a 2.6 GB file into ~325_000 syscalls
/// (measured: 1.7 s of the 2.8 s write).  One more buffer in front of
/// the file turns that into ~325.
const WRITE_BUF: usize = 8 << 20;

/// Soft cap on the bytes of L payload represented by in-flight chunks.
/// Chunks hold an Arrow copy AND its encoded form, so true transient
/// memory is a small multiple of this — still a few row groups, which
/// is the point.
const INFLIGHT_BYTES: usize = 512 << 20;

// ── options ────────────────────────────────────────────────────────────

/// Writer options, as parsed from the optional third argument.
pub struct Opts {
    comp: Compression,
    rg: usize,
    dict: bool,
    stats: bool,
}

impl Default for Opts {
    fn default() -> Self {
        // `none` is the DEFAULT codec (owner's call): the column
        // encodings carry the compression, and a file no reader has to
        // inflate is the one that composes with everything.
        Opts {
            comp: Compression::UNCOMPRESSED,
            rg: ROW_GROUP,
            dict: true,
            stats: true,
        }
    }
}

/// Wrong option value → one error naming the key, plus the L type of
/// what arrived when the tag is one we can name.
fn bad(key: &str, want: &str, v: &Val) -> String {
    match v {
        Val::Bad(t) => {
            format!("pq_write: opt `{key} wants a {want} (got type {t})")
        }
        _ => format!("pq_write: opt `{key} wants a {want}"),
    }
}

/// Parse the `opts` dict.  Every key is optional; an unknown key or a
/// wrongly typed value is an error that names the key.
unsafe fn parse_opts(opts: Option<K>) -> Result<Opts, String> {
    let mut o = Opts::default();
    let Some(d) = opts else { return Ok(o) };
    if kt(d) != XD {
        return Err("pq_write: opts must be a dict".into());
    }
    let keys = *v_k(d);
    let vals = *v_k(d).add(1);
    if vn(keys) != 0 && vt(keys) != KS {
        return Err("pq_write: opts keys must be symbols".into());
    }
    // codec+level fold together into one Compression, so collect first.
    let mut codec: Option<String> = None;
    let mut level: Option<i64> = None;
    for i in 0..vn(keys) as usize {
        let k = CStr::from_ptr(*v_s(keys).add(i))
            .to_str()
            .map_err(|_| "pq_write: opts key utf8")?;
        let v = val_at(vals, i);
        match k {
            "codec" => match v {
                Val::Sym(p) => {
                    codec = Some(
                        CStr::from_ptr(p)
                            .to_str()
                            .map_err(|_| "pq_write: opt `codec utf8")?
                            .to_string(),
                    )
                }
                _ => return Err(bad("codec", "symbol", &v)),
            },
            "level" => match v {
                Val::Int(n) => level = Some(n),
                _ => return Err(bad("level", "long", &v)),
            },
            "rg" => match v {
                Val::Int(n) if n > 0 => o.rg = n as usize,
                Val::Int(_) => {
                    return Err("pq_write: opt `rg must be > 0".into())
                }
                _ => return Err(bad("rg", "long", &v)),
            },
            "dict" => match v {
                Val::Bool(b) => o.dict = b,
                _ => return Err(bad("dict", "boolean", &v)),
            },
            "stats" => match v {
                Val::Bool(b) => o.stats = b,
                _ => return Err(bad("stats", "boolean", &v)),
            },
            other => {
                return Err(format!("pq_write: unknown opt `{other}"))
            }
        }
    }
    // Levels are a codec property: a codec that has none ignores the
    // key.  The narrowing is fallible on purpose — `level` arrives as a
    // 64-bit long, and a value that does not fit the codec's own type
    // has to be refused, not truncated into a level that happens to be
    // legal.
    let lv = |d: i64| level.unwrap_or(d);
    let range = |c: &str| format!("pq_write: opt `level out of range for `{c}");
    o.comp = match codec.as_deref() {
        None | Some("none") => Compression::UNCOMPRESSED,
        Some("zstd") => Compression::ZSTD(
            i32::try_from(lv(1))
                .ok()
                .and_then(|l| ZstdLevel::try_new(l).ok())
                .ok_or_else(|| range("zstd"))?,
        ),
        Some("snappy") => Compression::SNAPPY,
        Some("lz4") => Compression::LZ4_RAW,
        Some("gzip") => Compression::GZIP(
            u32::try_from(lv(6))
                .ok()
                .and_then(|l| GzipLevel::try_new(l).ok())
                .ok_or_else(|| range("gzip"))?,
        ),
        Some(c) => {
            return Err(format!(
                "pq_write: opt `codec `{c} (want \
                 `none`zstd`snappy`lz4`gzip)"
            ))
        }
    };
    Ok(o)
}

// ── encoding policy ────────────────────────────────────────────────────

/// The Parquet encoding chosen for one column.
#[derive(Clone, Copy, PartialEq)]
enum Enc {
    Plain,
    Delta,
    Dict,
    Rle,
}

/// Widen L element `i` of an integral/temporal column to i64, or None
/// for the L null sentinel (which the writer emits as a Parquet null
/// and which therefore says nothing about the value distribution).
unsafe fn probe(col: K, t: i16, i: usize) -> Option<i64> {
    Some(match t {
        KG => *v_g(col).add(i) as i64,
        KH => match *v_h(col).add(i) {
            NH => return None,
            v => v as i64,
        },
        KI | KD | KT => match *v_i(col).add(i) {
            NI => return None,
            v => v as i64,
        },
        // Every remaining type this is called for is 8 bytes wide;
        // anything else must not be read through an i64 pointer.
        KJ | KP | KN => match *v_j(col).add(i) {
            NJ => return None,
            v => v,
        },
        _ => return None,
    })
}

/// Choose the encoding for an integral/temporal column from a STRIDED
/// sample of at most `SAMPLE` values — rows `0, step, 2*step, ...` for
/// `step = n / SAMPLE`, which is a PREFIX when `n < 2 * SAMPLE`:
///   * monotonic (either direction) → DELTA_BINARY_PACKED.  Sorted
///     timestamps and dense counters shrink to the delta's bit width;
///     this is the TAQ `ts` column and every clock-like column.
///   * estimated cardinality under `DICT_CARD` → dictionary.  The
///     RLE-packed indices cost bits where PLAIN costs bytes and DELTA
///     costs the full span between neighbours (TAQ `size`: 999
///     distinct in 100M rows).
///   * otherwise → PLAIN.  Random wide integers make DELTA *larger*
///     than the values it encodes (the delta of two unrelated i64s
///     needs 65 bits), and a dictionary bigger still.
///
/// Cardinality comes from the sample's COLLISIONS, not its distinct
/// count: drawing `m` values from a population of `C` collides about
/// `m^2 / 2C` times, so `C ~ m^2 / 2(m - distinct)`.  That separates
/// 10^3 from 10^6 with 8192 probes, which counting distinct values
/// alone cannot.  It assumes the values are spread evenly through the
/// column AND that the stride sees enough of it; a column whose period
/// aliases with the stride can fool it, and so can one whose tail — or,
/// under `SAMPLE <= n < 2*SAMPLE`, whose whole second half — is not
/// what the prefix showed.  Being fooled only costs bytes: every
/// encoding here is lossless and every reader takes all of them.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn pick_int(col: K, t: i16, n: usize, dict: bool) -> Enc {
    if n < 2 {
        return Enc::Plain;
    }
    let m = SAMPLE.min(n);
    let step = (n / m).max(1);
    let mut v = Vec::with_capacity(m);
    for i in 0..m {
        if let Some(x) = probe(col, t, i * step) {
            v.push(x);
        }
    }
    if v.len() < 2 {
        return Enc::Plain;
    }
    let asc = v.windows(2).all(|w| w[0] <= w[1]);
    let desc = v.windows(2).all(|w| w[0] >= w[1]);
    if asc || desc {
        return Enc::Delta;
    }
    let (s, d) = (v.len(), v.iter().collect::<HashSet<_>>().len());
    let card = if m == n {
        d                                                                       // the sample IS the column: an exact count
    } else if d == s {
        usize::MAX                                                              // no collisions: far above the threshold
    } else {
        s * s / (2 * (s - d))
    };
    if card < DICT_CARD {
        // Without a dictionary the same shape is still delta-friendly:
        // few distinct values means small gaps between neighbours.
        return if dict { Enc::Dict } else { Enc::Delta };
    }
    Enc::Plain
}

/// The encoding for a column of L type `t`.
unsafe fn pick(col: K, t: i16, n: usize, o: &Opts) -> Enc {
    match t {
        KB => Enc::Rle,
        // Floats: PLAIN, always.  BYTE_STREAM_SPLIT only shuffles the
        // bytes, so with no codec behind it the file is byte-for-byte
        // the same size — and WITH one it is a trap: measured on the
        // TAQ set it takes zstd-1 from 2.33 to 2.25 GB but zstd-3 from
        // 1.45 to 2.25 GB and gzip from 1.47 to 2.22 GB, because
        // splitting the byte planes destroys the whole-value matches a
        // stronger compressor lives on (price/bid/ask hold 1_000_183
        // distinct doubles in 100M rows).
        KE | KF => Enc::Plain,
        KS => {
            if o.dict {
                Enc::Dict
            } else {
                Enc::Plain
            }
        }
        // KZ is computed (f64 days → ns), so the payload we would
        // sample is not the payload we write.
        KZ => Enc::Plain,
        _ => pick_int(col, t, n, o.dict),
    }
}

/// Apply one column's encoding to the writer properties.  Dictionary
/// encoding is a MODE, not an encoding: with it on, the fallback
/// encoding is what arrow-rs uses once the dictionary outgrows its page
/// limit, and naming a dictionary encoding explicitly is rejected.
fn set_enc(
    b: parquet::file::properties::WriterPropertiesBuilder,
    name: &str,
    e: Enc,
) -> parquet::file::properties::WriterPropertiesBuilder {
    let p = ColumnPath::new(vec![name.to_string()]);
    let e = match e {
        Enc::Dict => return b.set_column_dictionary_enabled(p, true),
        Enc::Plain => Encoding::PLAIN,
        Enc::Delta => Encoding::DELTA_BINARY_PACKED,
        Enc::Rle => Encoding::RLE,
    };
    b.set_column_dictionary_enabled(p.clone(), false)
        .set_column_encoding(p, e)
}

// ── L column → Arrow array ─────────────────────────────────────────────

/// The Arrow type each L column type is written as.  `sym_dict` picks
/// the SYMBOL spelling: the writer feeds arrow-rs a dictionary array
/// (no per-row string bytes), while the file's embedded Arrow schema
/// advertises plain Utf8 — the Parquet column is BYTE_ARRAY/String
/// either way, and readers should see a string column, not a
/// dictionary one.
fn arrow_type(t: i16, sym_dict: bool) -> Result<DataType, String> {
    Ok(match t {
        KB => DataType::Boolean,
        KG => DataType::UInt8,
        KH => DataType::Int16,
        KI => DataType::Int32,
        KJ => DataType::Int64,
        KE => DataType::Float32,
        KF => DataType::Float64,
        KS if sym_dict => DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ),
        KS => DataType::Utf8,
        KD => DataType::Date32,
        KT => DataType::Time32(TimeUnit::Millisecond),
        // KZ (f64 days since 2000) is WRITE-ONLY: emitted as
        // Timestamp[ns] so it reads back as KP.
        KP | KZ => DataType::Timestamp(TimeUnit::Nanosecond, None),
        KN => DataType::Duration(TimeUnit::Nanosecond),
        0 => return Err("nyi: list column".into()),
        _ => return Err(format!("nyi: column type {t}")),
    })
}

/// The L payload `[src, src+n)` AS an Arrow values buffer — no copy at
/// all: the encoder reads K memory directly.  Sound because the K table
/// is `pq_write`'s argument, which the host holds for the whole call,
/// and every array built from this is consumed by the task that built
/// it long before the call returns; the buffer's owner is a unit, so
/// dropping it frees nothing.  None when the payload is not aligned for
/// `T` (arrow ASSERTS on that) or is empty — the caller copies instead.
unsafe fn borrow_buf<T: ArrowNativeType>(
    src: *const T,
    n: usize,
) -> Option<ScalarBuffer<T>> {
    if n == 0 || src.align_offset(std::mem::align_of::<T>()) != 0 {
        return None;
    }
    let p = NonNull::new(src as *mut u8)?;
    let b = Buffer::from_custom_allocation(
        p,
        n * std::mem::size_of::<T>(),
        Arc::new(()),
    );
    Some(ScalarBuffer::new(b, 0, n))
}

/// L payload → Arrow values buffer: BORROWED when the layouts allow it
/// (they do for every fixed-width L type), copied only as a fallback.
unsafe fn vals_of<T: ArrowNativeType>(
    src: *const T,
    n: usize,
) -> ScalarBuffer<T> {
    match borrow_buf(src, n) {
        Some(b) => b,
        None => ScalarBuffer::from(
            std::slice::from_raw_parts(src, n).to_vec(),
        ),
    }
}

/// L payload → Arrow primitive array with no L null sentinel: the K
/// payload itself, no bitmap and no copy.
unsafe fn col_raw<A: ArrowPrimitiveType>(
    src: *const A::Native,
    n: usize,
) -> ArrayRef {
    Arc::new(PrimitiveArray::<A>::new(vals_of(src, n), None))
}

/// The null bitmap `valid` implies for `s`, or None when every value is
/// valid.  One pass, assembled a WORD at a time rather than a bit at a
/// time, and it never touches the values again after the caller's own
/// pass (or instead of it, when the values are borrowed).
fn null_bits<T>(s: &[T], valid: impl Fn(&T) -> bool) -> Option<NullBuffer> {
    let mut bits: Vec<u64> = Vec::with_capacity(s.len().div_ceil(64));
    let mut nulls = false;
    for c in s.chunks(64) {
        let mut w = 0u64;
        for (i, x) in c.iter().enumerate() {
            w |= (valid(x) as u64) << i;
        }
        // A full chunk is all-valid iff every bit is set; the last one
        // only owns its own `c.len()` bits.
        nulls |= w != (!0u64 >> (64 - c.len()));
        bits.push(w);
    }
    nulls.then(|| {
        NullBuffer::new(BooleanBuffer::new(
            Buffer::from_vec(bits),
            0,
            s.len(),
        ))
    })
}

/// L payload → Arrow primitive array.  With no epoch shift the values
/// are BORROWED — the encoder reads the K payload itself and the only
/// thing allocated is the null bitmap, one pass over the source.  A
/// shift (KD/KP) is the one case that must materialize a transformed
/// copy, and then the copy and the bitmap come out of the SAME pass.
/// An all-valid column carries no bitmap at all.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn col_prim<A: ArrowPrimitiveType>(
    src: *const A::Native,
    n: usize,
    sh: A::Native,
    valid: impl Fn(&A::Native) -> bool,
) -> ArrayRef
where
    A::Native: ArrowNativeTypeOp,
{
    let s = std::slice::from_raw_parts(src, n);
    if sh.is_zero() {
        let nb = null_bits(s, &valid);
        return Arc::new(PrimitiveArray::<A>::new(vals_of(src, n), nb));
    }
    // ONE pass, deliberately: splitting it into a clean vectorizable
    // value pass plus a bitmap pass was measured on the mac and LOST —
    // 8M KP rows went 17.0 ms to 18.7-20.0 ms, because a second walk of
    // a column that does not fit L2 costs more memory traffic than the
    // vectorization saves.  It only pays where the encoder is CPU
    // bound, and with any codec behind it the write is bytes bound.
    let mut v: Vec<A::Native> = Vec::with_capacity(n);
    let mut bits: Vec<u64> = Vec::with_capacity(n.div_ceil(64));
    let mut nulls = false;
    for c in s.chunks(64) {
        let mut w = 0u64;
        for (i, &x) in c.iter().enumerate() {
            w |= (valid(&x) as u64) << i;
            v.push(x.add_wrapping(sh));
        }
        nulls |= w != (!0u64 >> (64 - c.len()));
        bits.push(w);
    }
    let nb = nulls.then(|| {
        NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(bits), 0, n))
    });
    Arc::new(PrimitiveArray::<A>::new(ScalarBuffer::from(v), nb))
}

/// Hash for INTERNED POINTER keys: one multiply by the 64-bit golden
/// ratio plus a xor-shift, so the aligned (hence constant) low bits of
/// a symbol pointer still spread across hashbrown's bucket index.
/// SipHash — the default — would cost more than the work it guards.
#[derive(Default)]
struct PtrHash(u64);

impl Hasher for PtrHash {
    #[inline]
    fn write_usize(&mut self, v: usize) {
        let h = (v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = h ^ (h >> 32);
    }
    // Never used (the only key type is usize) but the trait demands it.
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0100_0000_01B3);
        }
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

type PtrMap = HashMap<usize, i32, BuildHasherDefault<PtrHash>>;

/// Symbol slice → `DictionaryArray<Int32>`: the L symbols are interned,
/// so pointer equality IS string equality.  Each distinct pointer is
/// resolved once (strlen + utf8 check) and lands in the dictionary; the
/// per-row work is one integer-hashed lookup and one i32 store, never a
/// string hash or a byte copy.  A pointer whose bytes are not utf8
/// becomes a NULL row (as it did when this built a StringArray).
unsafe fn col_sym(src: *const *mut c_char, n: usize) -> ArrayRef {
    let mut ids: PtrMap = PtrMap::default();
    let mut vals: Vec<&str> = Vec::new();
    let mut keys: Vec<i32> = Vec::with_capacity(n);
    let mut nulls: Option<BooleanBufferBuilder> = None;
    // A symbol column is routinely CLUSTERED — sorted by sym, or
    // partitioned by it — and a run of one pointer has one answer, so
    // the run's second row onward costs a compare instead of a probe.
    let mut last = (usize::MAX, 0i32);                                          // usize::MAX is no pointer
    for i in 0..n {
        let p = *src.add(i);
        let id = if p as usize == last.0 {
            last.1
        } else {
            *ids.entry(p as usize).or_insert_with(|| {
                match CStr::from_ptr(p).to_str() {
                    Ok(s) => {
                        vals.push(s);
                        vals.len() as i32 - 1
                    }
                    Err(_) => -1,
                }
            })
        };
        last = (p as usize, id);
        if id < 0 && nulls.is_none() {
            let mut b = BooleanBufferBuilder::new(n);
            b.append_n(i, true);
            nulls = Some(b);
        }
        if let Some(b) = nulls.as_mut() {
            b.append(id >= 0);
        }
        keys.push(id.max(0));
    }
    // A dictionary of nothing but null rows still needs a slot for the
    // key 0 those rows carry.
    if vals.is_empty() && !keys.is_empty() {
        vals.push("");
    }
    let nb = nulls.map(|mut b| NullBuffer::new(b.finish()));
    let k = Int32Array::new(ScalarBuffer::from(keys), nb);
    let v: ArrayRef = Arc::new(StringArray::from(vals));
    Arc::new(DictionaryArray::new(k, v))
}

/// One L column's rows `[off, off+n)` → Arrow array of `arrow_type`.
/// Slicing is pointer arithmetic on the K payload, so a task touches
/// only its own rows and nothing is built for the table as a whole.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn col_to_arrow(
    col: K,
    off: usize,
    n: usize,
) -> Result<ArrayRef, String> {
    let t = kt(col);
    Ok(match t {
        KB => {
            let s = v_g(col).add(off);
            let bb = BooleanBuffer::collect_bool(n, |i| unsafe {
                *s.add(i) != 0
            });
            Arc::new(BooleanArray::new(bb, None))
        }
        // KG/KE have no null sentinel at this boundary and KH's is
        // asymmetric: all three write fully valid (f32 NaN and 0Nh
        // alike go out as VALUES, no null scan), while a Parquet Int16
        // null still reads back as 0Nh.  Matches arrow_interop.
        KG => col_raw::<UInt8Type>(v_g(col).add(off), n),
        KH => col_raw::<Int16Type>(v_h(col).add(off), n),
        KE => col_raw::<Float32Type>(v_e(col).add(off), n),
        KI => col_prim::<Int32Type>(v_i(col).add(off), n, 0, |&x| x != NI),
        KJ => col_prim::<Int64Type>(v_j(col).add(off), n, 0, |&x| x != NJ),
        // f64: NaN IS the L float null → Parquet null.
        KF => {
            col_prim::<Float64Type>(v_f(col).add(off), n, 0., |x| {
                !x.is_nan()
            })
        }
        KS => col_sym(v_s(col).add(off), n),
        KD => {
            // Epoch shift 2000 → 1970, null-preserving; same for KP.
            // Values whose shifted form overflows the target width
            // (dates past i32::MAX-10957 days, timestamps past ~2262)
            // are unrepresentable in Parquet — written as null, never
            // as a wrapped-around wrong value.
            col_prim::<Date32Type>(v_i(col).add(off), n, DAY2000, |&x| {
                x != NI && x <= i32::MAX - DAY2000
            })
        }
        KT => col_prim::<Time32MillisecondType>(
            v_i(col).add(off),
            n,
            0,
            |&x| x != NI,
        ),
        KP => col_prim::<TimestampNanosecondType>(
            v_j(col).add(off),
            n,
            NS2000,
            |&x| x != NJ && x <= i64::MAX - NS2000,
        ),
        // Duration keeps raw ns.  Parquet has no duration logical type;
        // arrow-rs stores Int64 plus the embedded Arrow schema, so
        // arrow readers (incl. this crate and pyarrow) restore
        // Duration[ns] and L reads it back as KN.
        KN => col_prim::<DurationNanosecondType>(
            v_j(col).add(off),
            n,
            0,
            |&x| x != NJ,
        ),
        KZ => {
            let s = v_f(col).add(off);
            let a: TimestampNanosecondArray = (0..n)
                .map(|i| {
                    // NaN is the KZ null; values whose ns form falls
                    // outside i64 (pre-1677 / post-2262) are
                    // unrepresentable — null, never a wrapped value.
                    let f = *s.add(i) * 86_400e9;
                    if f.is_nan() || f < i64::MIN as f64 {
                        None
                    } else {
                        (f as i64).checked_add(NS2000)
                    }
                })
                .collect();
            Arc::new(a)
        }
        0 => return Err("nyi: list column".into()),
        _ => return Err(format!("nyi: column type {t}")),
    })
}

// ── footer metadata ────────────────────────────────────────────────────

/// base64 (standard alphabet, padded) — the ARROW:schema key-value
/// encoding.  Hand-rolled to keep the dependency set at arrow+parquet.
fn b64(data: &[u8]) -> String {
    const A: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for ch in data.chunks(3) {
        let b = [ch[0], *ch.get(1).unwrap_or(&0), *ch.get(2).unwrap_or(&0)];
        let w = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        for k in 0..4 {
            out.push(if k <= ch.len() {
                A[(w >> (18 - 6 * k) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// The ARROW:schema footer metadata ArrowWriter would have embedded:
/// length-prefixed legacy-IPC schema message, base64'd.  Readers use it
/// to restore Arrow-level types Parquet cannot express (Duration → KN).
fn arrow_schema_meta(schema: &Schema) -> KeyValue {
    let gen = IpcDataGenerator::default();
    let mut track = DictionaryTracker::new(true);
    let msg = gen
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut track,
            &IpcWriteOptions::default(),
        )
        .ipc_message;
    let mut buf = Vec::with_capacity(msg.len() + 8);
    buf.extend_from_slice(&[255, 255, 255, 255]);
    buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    buf.extend_from_slice(&msg);
    KeyValue::new(ARROW_SCHEMA_META_KEY.to_string(), b64(&buf))
}

// ── the (row group × column) work queue ────────────────────────────────

/// Shared queue state.  Tasks are numbered `rg * nc + col`, handed out
/// in that order, and consumed by the master in that same order — which
/// is what keeps the output bytes identical to a serial writer's.
struct Q {
    next: usize,
    head: usize,
    /// Column writers per row group, built when its first column is
    /// claimed and dropped when its last one is.
    wr: BTreeMap<usize, Vec<Option<ArrowColumnWriter>>>,
    done: HashMap<usize, ArrowColumnChunk>,
    err: Option<String>,
}

/// Queue + the two waits: workers block when the in-flight window is
/// full, the master blocks on the chunk it needs next.
struct Shared {
    q: Mutex<Q>,
    work: Condvar,
    master: Condvar,
}

impl Shared {
    /// Take the queue lock, ignoring poisoning: a thread that panicked
    /// mid-update leaves the queue inconsistent, but every reader is
    /// about to abandon it anyway — and panicking HERE would strand
    /// whoever is waiting.
    fn lk(&self) -> std::sync::MutexGuard<'_, Q> {
        self.q.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record the first failure and wake everyone to unwind.
    fn fail(&self, e: String) {
        self.lk().err.get_or_insert(e);
        self.work.notify_all();
        self.master.notify_all();
    }
}

/// Write L table `tbl` → Parquet at `path` with `opts` (see `parse_opts`).
pub fn write_table(
    tbl: K,
    path: &str,
    opts: Option<K>,
) -> Result<(), String> {
    unsafe {
        let o = parse_opts(opts)?;
        if kt(tbl) != XT {
            return Err("pq_write: not a table".into());
        }
        // XT payload[0] is the column dict; dict payload is [keys;vals].
        let dict = *v_k(tbl);
        let names = *v_k(dict);
        let colsl = *v_k(dict).add(1);
        let nc = vn(names) as usize;
        let cols: Vec<K> = (0..nc).map(|c| *v_k(colsl).add(c)).collect();
        let nrows = if nc > 0 { kn(cols[0]) as usize } else { 0 };

        // Schemas: `wf` is what the writer encodes (symbols as
        // dictionary arrays), `mf` is what the file advertises.
        let mut wf = Vec::with_capacity(nc);
        let mut mf = Vec::with_capacity(nc);
        let mut props = WriterProperties::builder()
            .set_compression(o.comp)
            .set_dictionary_enabled(o.dict)
            .set_max_row_group_size(o.rg)
            .set_statistics_enabled(if o.stats {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            });
        let mut width = 0usize;
        let mut seen: HashSet<&str> = HashSet::with_capacity(nc);
        for (c, &col) in cols.iter().enumerate() {
            let nm = CStr::from_ptr(*v_s(names).add(c))
                .to_str()
                .map_err(|_| "pq_write: column name utf8")?;
            if !seen.insert(nm) {
                // `set_enc` keys its properties by NAME, so duplicates
                // would collapse onto the last column's encoding — and
                // this library's own reader refuses the file it writes
                // ("duplicate column").  A write that cannot round-trip
                // is refused here instead of on the way back in.
                return Err(format!("pq_write: duplicate column {nm}"));
            }
            if kn(col) as usize != nrows {
                return Err(format!(
                    "pq_write: column {nm} length {} != {nrows}",
                    kn(col)
                ));
            }
            let t = kt(col);
            // nullable=true unconditionally: L cannot promise absence
            // of sentinels, and readers treat it as "may contain".
            wf.push(Field::new(nm, arrow_type(t, true)?, true));
            mf.push(Field::new(nm, arrow_type(t, false)?, true));
            props = set_enc(props, nm, pick(col, t, nrows, &o));
            // Bytes one row of this column occupies, for sizing the
            // in-flight window below: `nt` is the host's own table and
            // stream.rs already treats it as authoritative.  Only the
            // widest column matters there, so it is a bound, not an
            // accounting.
            width = width.max(nt(t as u32) as usize);
        }
        let schema = Arc::new(Schema::new(wf));
        let props = Arc::new(
            props
                .set_key_value_metadata(Some(vec![arrow_schema_meta(
                    &Schema::new(mf),
                )]))
                .build(),
        );
        let pq_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)
            .ctx("pq_write")?;
        // Build beside the target, never on it: the reader sees either
        // the previous file or the finished one, never a prefix of this
        // one.
        let (tmp, mut scratch) = crate::Scratch::new(path, false);
        let file =
            File::create(&tmp).ctx(&format!("pq_write: {path}"))?;
        let mut fw = SerializedFileWriter::new(
            BufWriter::with_capacity(WRITE_BUF, file),
            pq_schema.root_schema_ptr(),
            props.clone(),
        )
        .ctx("pq_write")?;

        let nrg = nrows.div_ceil(o.rg);
        let ntasks = nrg * nc;
        if ntasks > 0 {
            let nthreads = crate::pool::nthreads().min(ntasks).max(1);
            // In-flight window: enough to keep every worker fed plus a
            // row group of slack, capped so the chunks we are holding
            // stay a bounded slice of the table.
            let per = (o.rg.min(nrows) * width).max(1);
            let window = (nthreads + nc)
                .min((INFLIGHT_BYTES / per).max(nthreads))
                .max(1);
            let sh = Shared {
                q: Mutex::new(Q {
                    next: 0,
                    head: 0,
                    wr: BTreeMap::new(),
                    done: HashMap::new(),
                    err: None,
                }),
                work: Condvar::new(),
                master: Condvar::new(),
            };
            // The master runs HERE, on the calling thread, while the
            // pool encodes: a panic in a worker would otherwise leave
            // it waiting for a chunk that is never coming, so the
            // worker turns its own panic into the queue's error.
            let body = || {
                let r = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| {
                        worker(
                            &sh, &cols, &schema, &pq_schema, &props, nc,
                            nrows, o.rg, ntasks, window,
                        )
                    }),
                );
                if r.is_err() {
                    sh.fail("pq_write: worker panic".into());
                }
            };
            let (r, _) = crate::pool::run(nthreads, &body, || {
                master(&sh, &mut fw, nrg, nc)
            });
            r?;
        }
        fw.close().ctx("pq_write")?;
        // UNLINK FIRST, then rename.  Renaming ONTO an existing file is
        // what ext4's auto_da_alloc heuristic watches for: it flushes
        // the whole new file synchronously inside rename(2) before it
        // returns — roughly DOUBLING the write on the box (README
        // carries the measured pair; one copy of a number is the only
        // way to keep two from drifting apart).  Unlinking the target
        // first turns the rename into a plain create-and-link.  The
        // replace stops being one atomic step for the microsecond in
        // between, but no reader can ever see a PREFIX of the new file:
        // it sees the old inode, then nothing, then the finished one —
        // and a crash inside that window leaves NO file at `path`, the
        // finished data still under its `.tmp` name.
        let _ = std::fs::remove_file(path);                                     // ENOENT is the normal case
        std::fs::rename(&tmp, path)
            .ctx(&format!("pq_write: {path}"))?;
        scratch.keep();
    }
    Ok(())
}

/// Encode chunks until the queue is empty or someone fails.  Everything
/// expensive — array build, encode, compress — happens outside the lock.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn worker(
    sh: &Shared,
    cols: &[K],
    schema: &SchemaRef,
    pq: &parquet::schema::types::SchemaDescriptor,
    props: &Arc<WriterProperties>,
    nc: usize,
    nrows: usize,
    rg: usize,
    ntasks: usize,
    window: usize,
) {
    loop {
        // Claim the next task, and with it this row group's writer.
        let claim = {
            let mut q = sh.lk();
            loop {
                if q.err.is_some() || q.next >= ntasks {
                    return;
                }
                if q.next < q.head + window {
                    break;
                }
                q = sh.work.wait(q).unwrap_or_else(|e| e.into_inner());
            }
            let i = q.next;
            let (g, c) = (i / nc, i % nc);
            if c == 0 {
                match get_column_writers(pq, props, schema) {
                    Ok(w) => {
                        q.wr.insert(g, w.into_iter().map(Some).collect());
                    }
                    Err(e) => {
                        drop(q);
                        sh.fail(format!("pq_write: {e}"));
                        return;
                    }
                }
            }
            q.next = i + 1;
            let w = q.wr.get_mut(&g).and_then(|v| v.get_mut(c)?.take());
            if c + 1 == nc {
                q.wr.remove(&g);
            }
            match w {
                Some(w) => (i, g, c, w),
                None => {
                    drop(q);
                    sh.fail("pq_write: writer lost".into());
                    return;
                }
            }
        };
        let (i, g, c, mut w) = claim;
        let off = g * rg;
        let len = rg.min(nrows - off);
        let r = (|| -> Result<ArrowColumnChunk, String> {
            let a = unsafe { col_to_arrow(cols[c], off, len)? };
            for leaf in
                compute_leaves(schema.field(c), &a).ctx("pq_write")?
            {
                w.write(&leaf).ctx("pq_write")?;
            }
            w.close().ctx("pq_write")
        })();
        match r {
            Ok(chunk) => {
                let mut q = sh.lk();
                q.done.insert(i, chunk);
                sh.master.notify_one();
            }
            Err(e) => {
                sh.fail(e);
                return;
            }
        }
    }
}

/// Append finished chunks in (row group, column) order — the only step
/// that touches the file, and the reason the layout is deterministic.
/// Every early return goes through `fail` first: workers parked on the
/// in-flight window must be woken, or the scope's join never returns.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn master(
    sh: &Shared,
    fw: &mut SerializedFileWriter<BufWriter<File>>,
    nrg: usize,
    nc: usize,
) -> Result<(), String> {
    let stop = |e: String| -> String {
        sh.fail(e.clone());
        e
    };
    for g in 0..nrg {
        let mut rgw = fw
            .next_row_group()
            .map_err(|e| stop(format!("pq_write: {e}")))?;
        for c in 0..nc {
            let i = g * nc + c;
            let chunk = {
                let mut q = sh.lk();
                loop {
                    if let Some(k) = q.done.remove(&i) {
                        // One consumed chunk frees exactly one window
                        // slot, so exactly one worker needs waking.
                        q.head = i + 1;
                        sh.work.notify_one();
                        break k;
                    }
                    if let Some(e) = &q.err {
                        return Err(e.clone());
                    }
                    q = sh
                        .master
                        .wait(q)
                        .unwrap_or_else(|e| e.into_inner());
                }
            };
            chunk
                .append_to_row_group(&mut rgw)
                .map_err(|e| stop(format!("pq_write: {e}")))?;
        }
        rgw.close().map_err(|e| stop(format!("pq_write: {e}")))?;
    }
    // Every task is consumed, so every worker's next look at the queue
    // ends it — but a worker parked on a full window has to be woken to
    // take that look, and the scope's join is what would hang.
    sh.work.notify_all();
    Ok(())
}
