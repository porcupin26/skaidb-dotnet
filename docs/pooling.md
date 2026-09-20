# Pooling

`SkaidbConnectionPool` keeps a bounded set of idle connections built from one
connection string, for code that runs statements from several threads or
does not want to pay a handshake per request.

```csharp
using var pool = new SkaidbConnectionPool(
    "Seeds=h1:7000,h2:7000;User=app;Password=secret;Database=app;TlsCa=/etc/skaidb/ca.crt",
    maxsize: 8);

// Callback form: checked out for the callback, returned afterwards, always.
long count = pool.WithConnection(c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "SELECT count(*) FROM t";
    return (long)cmd.ExecuteScalar()!;
});

// Manual form.
var conn = pool.Acquire();
try
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "INSERT INTO t (id) VALUES (?)";
    cmd.Parameters.Add(1L);
    cmd.ExecuteNonQuery();
}
finally
{
    pool.Release(conn);
}
```

## Semantics

- `maxsize` (default 10) bounds the connections kept **idle**, not the number
  checked out. A burst opens extras; on `Release()` the surplus is disposed.
- `Acquire()` pops an idle connection and checks `IsUsable` — which includes
  a zero-timeout poll of the socket, so a connection the server closed while
  it sat idle is disposed and the next one tried, and a fresh one is opened
  when none is left. A fresh connection runs the full `Open()` (seed walk,
  handshake, Hello, `USE`).
- `Release()` pushes the connection back if `IsUsable` and the idle set is
  below `maxsize`; otherwise it disposes it. A connection that hit a transport
  error, or whose `Stream()` was abandoned undisposed, is therefore never
  handed to another caller.
- `WithConnection()` is `Acquire()` + the callback + `Release()` in a
  `finally`. Do not let a lazy `Stream()` enumerable escape the callback.
- `Dispose()` closes every idle connection; later `Acquire()` calls throw
  `pool is closed`. Connections checked out at that moment are disposed when
  released.
- `maxsize` below 1 throws.

## What a pool does not do

- It does not limit concurrency: it never blocks a caller waiting for a
  connection. Bound your own parallelism if the server needs it.
- It does not retry statements. A statement that fails with a transport
  error throws to the caller; the connection is disposed on release and the
  next `Acquire()` gives a healthy one.
- It does not health-check idle connections on a timer; the check happens on
  `Acquire()`.

The runnable [`examples/Pool`](../examples/Pool/Program.cs) drives a pool
from `Parallel.For`.
