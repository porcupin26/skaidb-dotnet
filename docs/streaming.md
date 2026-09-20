# Streaming

## `conn.Stream(sql, consistency?)`

`ExecuteReader()` receives the whole result in one frame and holds it in
memory; the server also bounds such a scan with its scan budgets. `Stream()`
sends the statement with the streaming opcode: the server answers with a
header (the column names), then the rows in chunks, then an end marker, and
the driver yields one row at a time holding one chunk:

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
  i.e. after the first `MoveNext()`.
- The optional consistency overrides the connection's for this statement.
- It takes **no parameters**: the streaming opcode carries SQL text only.
  Quote values yourself or select a wider range and filter client-side.
- A non-row statement (`INSERT`, DDL) streamed this way yields nothing.
- An error *before* the header is an ordinary statement error thrown from
  the first `MoveNext()`. An error *after* the header (a node dying mid-scan,
  a scan budget tripping) is thrown from the `MoveNext()` that meets it; the
  rows already yielded are valid and the connection stays usable.
- On a cluster, name the columns: a bare `SELECT *` is only executed page by
  page at consistency `One`; a named column list streams at any level.
- A server without the streaming opcode answers `server does not support
  streaming: …`.

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
