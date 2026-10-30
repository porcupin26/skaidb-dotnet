# Streaming

## `conn.Stream(sql, parameters?, consistency?)`

`ExecuteReader()` receives the whole result in one frame and holds it in
memory; the server also bounds such a scan with its scan budgets. `Stream()`
sends the statement with the streaming opcode: the server answers with a
header (the column names), then the rows in chunks, then an end marker, and
the driver yields one row at a time holding one chunk. The server executes a
plain `SELECT` page by page, and no scan budget applies to a streamed page
(server ≥ 0.330; `EXPLAIN` says whether a statement pages, and the rules are
under "Streamed queries" in the
[query syntax reference](https://skaidb.org/docs/query_syntax.html)):

```csharp
long n = 0;
foreach (object?[] row in conn.Stream("SELECT id, v FROM readings"))
{
    n++;
    var id = (long)row[0]!;
    var v = (double)row[1]!;
}
Console.WriteLine($"{n} rows, columns {string.Join(",", conn.StreamColumns)}");
```

- Rows are `object?[]` cell arrays in column order (see [types.md](types.md)
  for the values); NULL is `null` here, not `DBNull`.
- `conn.StreamColumns` holds the column names once the header has arrived,
  i.e. after the first `MoveNext()`. Each `Stream()` starts it empty, so it
  stays empty for a statement that returns no rows or fails before its
  header.
- The optional consistency overrides the connection's for this statement.
- A list of values binds the statement's `?` placeholders; see
  [Parameters](#parameters).
- A non-row statement (`INSERT`, DDL) streamed this way yields nothing; for
  a mutation `conn.StreamAffected` then holds the affected-row count (it is
  `null` for rows and DDL).
- A `CALL` whose procedure body `EMIT`s result sets is answered with all of
  them in one frame, as `ExecuteReader()` receives it: the enumeration
  yields the first set's rows (`conn.StreamColumns` its columns), and
  `conn.StreamResultSets` is the reader `ExecuteReader()` would return —
  on the first set, `NextResult()` walking the rest; `null` for every other
  statement. Nothing is left in flight, so the connection is free before
  the first row arrives:

  ```csharp
  foreach (var row in conn.Stream("CALL report()")) { /* the first set */ }
  var r = conn.StreamResultSets!;
  do { while (r.Read()) { /* ... */ } } while (r.NextResult());
  ```
- An error *before* the header is an ordinary statement error thrown from
  the first `MoveNext()`. An error *after* the header (a node dying mid-scan,
  a scan budget tripping) is thrown from the `MoveNext()` that meets it; the
  rows already yielded are valid and the connection stays usable.
- A server without the streaming opcode answers `server does not support
  streaming: …`.

## Parameters

`conn.Stream(sql, parameters, consistency?)` binds the statement's `?`
placeholders in order, typed, with the values a command's `Parameters` take
([types.md](types.md)) — arrays and documents included:

```csharp
foreach (var row in conn.Stream("SELECT id, ts, body FROM events WHERE ts >= ? AND ts < ?", [start, end]))
{
    Export(row);
}

var clicks = conn.Stream("SELECT id FROM events WHERE kind = ? AND tags = ?",
                         new object?[] { "click", new[] { "a", "b" } }, SkaidbConsistency.One);
```

- `parameters` is an `IReadOnlyList<object?>`, like an `ExecuteBatch` row:
  a collection expression, an `object?[]`, a `List<object?>`. Placeholders
  are `?` only; `@name` and `$N` belong to `SkaidbCommand`.
- A consistency stays the last argument, by position or by name, and is
  never taken for a parameter: `Stream(sql, null)` means the connection's
  default consistency.
- The values travel in `OP_QUERY_STREAM_PARAMS` (the SQL, then the
  parameter vector `OP_EXECUTE` carries), which needs a server ≥ 0.330. An
  older server answers `server does not support parameterised streaming
  (skaidb >= 0.330): …`, thrown from the first `MoveNext()`; the connection
  stays usable. There is no client-side text fallback.
- No values (`null` or an empty list) send the parameterless request, which
  every server takes.
- The server checks the values against the placeholders: a count that does
  not match, or a statement kind that cannot be prepared, is an ordinary
  statement error. A value the driver cannot bind (`cannot bind
  NaN/Infinity`, …) throws before anything is sent.
- The list is read when the enumeration starts, like the rest of the
  statement.

## The abandon rule

The connection is **busy for the whole stream**: the protocol allows no other
request on it until the end marker (or an error) has been read. While a
stream is open, `IsUsable` is false and any statement on the connection —
from any thread — throws `connection is busy streaming: …` rather than wait.

Disposing the enumerator is what ends a stream early. `foreach` disposes on
every exit — `break`, `return`, an exception thrown from the loop body. On
early disposal the driver reads and discards what the server still owes, up
to 64 frames; past that it drops the connection instead (`IsUsable` false,
the next statement re-dials transparently), rather than drag a whole
abandoned export through the socket. Either way the next statement reads its
own reply.

```csharp
foreach (var row in conn.Stream("SELECT id FROM big"))
{
    if ((long)row[0]! > 100) break;      // fine: foreach disposes the enumerator
}
```

A hand-rolled enumerator must be disposed explicitly:

```csharp
using (var it = conn.Stream("SELECT id FROM big").GetEnumerator())
{
    if (it.MoveNext()) Console.WriteLine(it.Current[0]);
}   // Dispose() here releases the connection
```

An enumerator that is neither exhausted nor disposed — created and dropped —
claims the connection for good: .NET runs no iterator `finally` at collection
time. That connection reports `IsUsable` false, a pool discards it, and every
statement on it throws. Never let one go out of scope without disposing it.

Do not return the enumerable out of `SkaidbConnectionPool.WithConnection`:
it is lazy, and the connection would go back to the pool before the first
row is read. Enumerate inside the callback.

## Following a stream

For a table with a `CREATE STREAM`, `Subscribe()` yields the stream's events
as they are captured, forever:

```csharp
foreach (var ev in conn.Subscribe("big_orders", after: lastSeenId, pollMs: 500))
{
    Console.WriteLine($"{ev.Id} {ev.Op} key={ev.Key} ts={ev.Ts}");
    var doc = ev.Doc as Dictionary<string, object?>;      // the row, for put events
    Save(ev.Id);                                           // pass it back as `after` to resume exactly here
}
```

`StreamEvent` is a record `(string Id, string Op, object? Key, object? Ts,
object? Doc)`. `Subscribe()` polls the stream's log table (`_stream_<name>`)
with a keyset cursor, 500 events per page, sleeping `pollMs` when a page is
empty. `Id` is the position — an opaque **string** that sorts in log order,
so `after` takes a saved `Id` or `null` for the beginning.

To stop, `break` out of the loop or dispose the enumerator. The connection
is idle between polls (each page is an ordinary statement), so a stop takes
effect within `pollMs`, and other statements on the same connection are
allowed between pages — but a writer is better off on its own connection,
as in [`examples/Subscribe`](../examples/Subscribe/Program.cs).

For push delivery subscribe to `$stream/<db>/<name>` with any MQTT client
instead — the events are identical.
