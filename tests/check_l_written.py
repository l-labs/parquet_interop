# check_l_written.py — pyarrow reads the file L wrote (suite test 18)
# and asserts schema + exact values.  Run AFTER tests/test_parquet.q:
#   uv run --with pyarrow tests/check_l_written.py
import datetime as dt
import math

import pyarrow.parquet as pq

F = "/tmp/pq_l_written.parquet"
t = pq.read_table(F)
s = t.schema

# Schema: the L→Parquet type map, including timestamp[ns] for KP and
# duration[ns] (restored from the embedded Arrow schema) for KN.
assert str(s.field("a").type) == "int32", s.field("a").type
assert str(s.field("b").type) == "double", s.field("b").type
assert str(s.field("s").type) == "string", s.field("s").type
assert str(s.field("bl").type) == "bool", s.field("bl").type
assert str(s.field("ts").type) == "timestamp[ns]", s.field("ts").type
assert str(s.field("dn").type) == "duration[ns]", s.field("dn").type

d = t.to_pydict()

# Values: 0Ni / 0n / 0Np / 0Nn all surface as Python None.
assert d["a"] == [1, None, 3], d["a"]
assert d["b"][0] == 1.5 and d["b"][2] == 3.5, d["b"]
assert d["b"][1] is None, d["b"]
assert d["s"] == ["x", "y", "z"], d["s"]
assert d["bl"] == [True, False, True], d["bl"]

# Epoch-2000 → epoch-1970 conversion: L wrote 2020.01.01D and
# 2020.01.02D; pyarrow must see those exact instants, and the null
# timestamp must be None.
assert d["ts"][0] == dt.datetime(2020, 1, 1), d["ts"]
assert d["ts"][1] == dt.datetime(2020, 1, 2), d["ts"]
assert d["ts"][2] is None, d["ts"]

# Durations: 1s and 3s in ns, with the middle value null.
assert d["dn"][0] == dt.timedelta(seconds=1), d["dn"]
assert d["dn"][1] is None, d["dn"]
assert d["dn"][2] == dt.timedelta(seconds=3), d["dn"]

# No NaN smuggled through where nulls were intended.
assert not any(isinstance(v, float) and math.isnan(v) for v in d["b"])

# The DEFAULT write is UNCOMPRESSED with light column encodings: no
# block codec, dictionary-encoded symbols, DELTA for the integral and
# temporal columns, PLAIN for the floats, RLE for the booleans.  All of
# it is ordinary Parquet — pyarrow decoded every column above.
m = pq.ParquetFile(F).metadata
rg = m.row_group(0)
cols = {rg.column(i).path_in_schema: rg.column(i)
        for i in range(rg.num_columns)}
assert {c.compression for c in cols.values()} == {"UNCOMPRESSED"}, \
    {c.compression for c in cols.values()}
assert "RLE_DICTIONARY" in cols["s"].encodings, cols["s"].encodings
assert "DELTA_BINARY_PACKED" in cols["a"].encodings, cols["a"].encodings
assert "PLAIN" in cols["b"].encodings, cols["b"].encodings
assert cols["bl"].encodings == ("RLE",), cols["bl"].encodings
# Chunk statistics are on by default: min/max/null_count for every one.
assert all(c.statistics is not None for c in cols.values())
assert cols["a"].statistics.null_count == 1, cols["a"].statistics

print("check_l_written: all assertions passed")
