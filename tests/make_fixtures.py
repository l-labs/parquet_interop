# make_fixtures.py — pyarrow-written Parquet fixtures for tests 13-16.
# Run: uv run --with pyarrow tests/make_fixtures.py
import pyarrow as pa
import pyarrow.parquet as pq

# 13: plain — no dictionary, no compression; exercises the raw decode
# path plus int/float nulls and a nulled timestamp[ns].
plain = pa.table({
    "a": pa.array([1, 2, 3], pa.int32()),
    "b": pa.array([1.5, 2.5, 3.5], pa.float64()),
    "s": pa.array(["x", "y", "z"], pa.string()),
    "bl": pa.array([True, False, True], pa.bool_()),
    "ts": pa.array([1577836800000000000, 1577923200000000000, None],
                   pa.timestamp("ns")),          # 2020-01-01, -02, null
    "ni": pa.array([1, None, 3], pa.int32()),
    "nf": pa.array([1.5, None, 3.5], pa.float64()),
})
pq.write_table(plain, "/tmp/pq_py_plain.parquet",
               use_dictionary=False, compression="none")

# 14: dictionary-encoded strings — exercises the dict-page decode path.
dic = pa.table({
    "s": pa.array(["a", "b", "a", "c", "b", "a"], pa.string()),
    "v": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
})
pq.write_table(dic, "/tmp/pq_py_dict.parquet",
               use_dictionary=True, compression="none")

# 15: zstd-compressed — exercises the decompression path.
zst = pa.table({
    "a": pa.array(range(1000), pa.int64()),
    "f": pa.array([i + 0.5 for i in range(1000)], pa.float64()),
})
pq.write_table(zst, "/tmp/pq_py_zstd.parquet", compression="zstd")

# 16: nested list column — must be REJECTED by pq_read with 'nyi.
nested = pa.table({
    "lst": pa.array([[1, 2], [3], []], pa.list_(pa.int64())),
})
pq.write_table(nested, "/tmp/pq_py_nested.parquet")

print("fixtures written: plain, dict, zstd, nested")
