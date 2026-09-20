# Types

## Value mapping

Results decode from skaidb's typed wire values; parameters encode back to
them when a statement is prepared (which is the normal path whenever a
command has parameters).

| skaidb type | Result value (`GetValue`) | Accepted as a parameter |
|---|---|---|
| Null | `DBNull.Value`; `IsDBNull(i)` is true | `null`, `DBNull.Value` |
| Bool | `bool` | `bool` |
| Int (64-bit) | `long` | `sbyte`, `byte`, `short`, `ushort`, `int`, `uint`, `long`; `ulong` and `BigInteger` when within the 64-bit range |
| Float (64-bit) | `double` | `float`, `double` (NaN and ±Infinity are refused: `cannot bind NaN/Infinity`) |
| Decimal | `decimal` when the mantissa fits 96 bits and the scale is ≤ 28; otherwise an exact `string` such as `"1267650600228229401496703205.376"` | `decimal` (exact: mantissa and scale, trailing zeros kept); `ulong`/`BigInteger` beyond 64 bits (scale 0, up to 128 bits) |
| String | `string` | `string`, `char` |
| Bytes | `byte[]` | `byte[]` |
| Uuid | `Guid` | `Guid` (sent in RFC 4122 byte order, not `Guid.ToByteArray()` order) |
| Timestamp | `DateTimeOffset`, offset zero, millisecond precision (may predate 1970) | `DateTimeOffset` (any offset; the instant is kept), `DateTime` (`Utc` and `Unspecified` kinds are taken as UTC, `Local` is converted) |
| Array | `object?[]` of mapped values | any `IEnumerable` that is not a `string`, `byte[]` or dictionary (`object[]`, `List<T>`, `T[]`…), nested |
| Document | `Dictionary<string, object?>`, keys in the server's order: the server stores a document with its keys sorted, so that is how they come back, whatever order you bound them in | any `IDictionary` whose keys are `string` (`Dictionary<string, object?>`…); non-string keys throw |

Notes:

- `GetDecimal(i)` converts a decimal-string fallback with the invariant
  culture; `GetValue(i)` gives you the `string` so nothing is rounded.
- `GetInt32(i)` on a `long` that does not fit throws `OverflowException`
  (from `Convert`).
- A document's keys come back sorted (`{"z":1,"a":2}` reads back as `a`, `z`):
  the server canonicalises documents on write, and the driver keeps the
  order it receives. Look keys up by name; do not rely on enumeration order
  matching the order you inserted.
- A `byte[]` result is a copy; mutating it does not affect the driver.
- `Guid` round-trips exactly: the driver encodes big-endian on the wire and
  builds the `Guid` from its canonical string form on the way back.
- In the client-side fallback (statements the server will not prepare, e.g.
  `USE ?`), values are rendered as SQL literals: numbers as digits, strings
  quoted with `'` doubled, `byte[]` as a hex string, `Guid` as its `D` form
  in quotes, `DateTimeOffset`/`DateTime` as epoch milliseconds; arrays and
  documents are refused (`cannot bind value of type …`).

## Reading a typed row

```csharp
using var r = cmd.ExecuteReader();
while (r.Read())
{
    long id = r.GetInt64(0);
    string name = r.GetString(1);
    decimal? amount = r.IsDBNull(2) ? null : r.GetDecimal(2);
    var tags = (object?[])r.GetValue(3);                       // Array
    var meta = (Dictionary<string, object?>)r.GetValue(4);     // Document
    DateTimeOffset at = r.GetDateTimeOffset(5);
}
```

## Binding a typed row

```csharp
cmd.CommandText = "INSERT INTO t (id, name, amount, tags, meta, at, raw, ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
cmd.Parameters.Add(1L);
cmd.Parameters.Add("Ada");
cmd.Parameters.Add(19.99m);
cmd.Parameters.Add(new[] { "a", "b" });
cmd.Parameters.Add(new Dictionary<string, object?> { ["k"] = 1L, ["nested"] = new[] { 1L, 2L } });
cmd.Parameters.Add(DateTimeOffset.UtcNow);
cmd.Parameters.Add(new byte[] { 0xde, 0xad });
cmd.Parameters.Add(Guid.NewGuid());
```

Note that `BY`, `AT` and other SQL keywords cannot be used as bare column
names; the server answers a prepare of such a statement with a parse error,
which the driver throws as-is.
