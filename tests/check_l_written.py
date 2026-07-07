# check_l_written.py — pyarrow reads the file L wrote (suite test 18)
# and asserts schema + exact values.  Run AFTER tests/test_parquet.q:
#   uv run --with pyarrow tests/check_l_written.py
import datetime as dt
import math

import pyarrow.parquet as pq

t = pq.read_table("/tmp/pq_l_written.parquet")
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

print("check_l_written: all assertions passed")
