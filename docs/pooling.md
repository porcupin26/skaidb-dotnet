# Pooling

`SkaidbConnectionPool` keeps a bounded set of idle connections built from one
connection string, for code that runs statements from several threads or
does not want to pay a handshake per request, and spreads them across the
cluster's members.

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

## API

```csharp
new SkaidbConnectionPool(string connectionString, int maxsize = 10);
new SkaidbConnectionPool(string connectionString, SkaidbPoolOptions options);
SkaidbConnection Acquire();
SkaidbConnection Acquire(object? affinity);
void Release(SkaidbConnection c);
T WithConnection<T>(Func<SkaidbConnection, T> work);
T WithConnection<T>(object? affinity, Func<SkaidbConnection, T> work);
IReadOnlyList<SkaidbPoolMember> Members();
void Dispose();
```

Every connection is built from the connection string, so pooled connections
inherit TLS, the session database, the timeout and the default consistency.
Its `Host`/`Seeds` entries are the pool's starting members (the seeds). A
pooled connection that loses its transport re-dials in the pool's order:
usable members first, the one it lost last.

## Balancing across members

By default (`SkaidbLoadBalancing.Random`) each new connection dials the
members in random order and stays on the member it reached — fine for many
clients, but one process can end up with most of its connections on one
member. Pick a policy to spread a single process's load:

```csharp
using var pool = new SkaidbConnectionPool(
    "Seeds=db1:7000,db2:7000,db3:7000;User=app;Password=secret;Database=app",
    new SkaidbPoolOptions
    {
        MaxSize = 8,
        LoadBalancing = SkaidbLoadBalancing.LeastLatency,   // or RoundRobin
        Discover = true,                                     // learn members from the cluster
        ReadYourWrites = TimeSpan.FromSeconds(2),            // see below
        IdleOrder = SkaidbIdleOrder.Fifo,
    });
```

| `SkaidbPoolOptions` | Default | What it does |
|---|---|---|
| `MaxSize` | `10` | The cap on idle connections kept (see below). |
| `LoadBalancing` | `Random` | `RoundRobin`: checkouts rotate over the usable members in endpoint order. `LeastLatency`: of two members picked at random, the one with the lower round-trip average × (checked-out connections + 1) — near and idle members win; a member not measured in the last 2 s scores 0, so a lightly loaded pool keeps re-measuring every member. |
| `IdleOrder` | `Lifo` | `Fifo` hands out the longest-idle connection first, so every idle connection keeps being used and a server-side idle timeout never finds one. |
| `Discover` | `false` | Reads the members with `SHOW ENDPOINTS` (server ≥ 0.327; no privilege needed) when a member is first reached and every `RefreshInterval` after. New members are added, members that left are dropped (seeds are kept, marked `Unlisted`), and members the server reports `resyncing` or `unreachable` get no new connections until they are `ready` again — re-checked every 5 s while any is — and then start with a fresh latency average. An older or standalone server leaves the seeds as they are and discovery stops. |
| `RefreshInterval` | 60 s | Time between discovery refreshes. |
| `EjectErrors` | `3` | Consecutive transport failures (dial, I/O, timeout, capacity refusal) that eject a member. Statement errors and refused logins do not count. |
| `EjectLatencyFactor`, `EjectLatencyFloor` | `3.0`, 50 ms | A member whose round-trip average is above factor × the other members' median **and** more than the floor above it is ejected (after 10 samples on it and on another member). A factor of `0` disables this. |
| `EjectBackoff`, `EjectBackoffMax` | 5 s, 60 s | How long an ejection lasts, doubling per repeat; a member healthy for `EjectBackoffMax` after a re-admission starts over at the base. Afterwards a background timer probes the member (dials it alone and runs one statement) and re-admits it; the probe connection joins the idle set. The last usable member is never ejected. |
| `ReadYourWrites` | `TimeSpan.Zero` | How long after a write checkouts with the same affinity key go to the member that took the write. Zero turns it off. |

An invalid option (an undefined enum value, `MaxSize` or `EjectErrors`
below 1, a negative duration) throws `SkaidbException` from the constructor.

**Latency** is an exponentially weighted average of each member's round
trips (request sent to first response frame): a sample moves it 5 %, and no
sample counts for more than 10 × the larger of the average and the ejection
floor, so one slow statement barely moves it while a member that stays slow
is ejected within a few dozen statements.

**Read-your-writes.** The member that coordinated a write reads it back at
any consistency, `One` included; another member may not have applied it yet.
With `ReadYourWrites` set, pass an affinity key — a user or session id —
when you check out:

```csharp
pool.WithConnection(userId, c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "INSERT INTO notes (id, owner, body) VALUES (?, ?, ?)";
    cmd.Parameters.Add(noteId); cmd.Parameters.Add(userId); cmd.Parameters.Add(body);
    return cmd.ExecuteNonQuery();
});
// ... the next request for the same user, within the window, lands on the same member:
string? text = pool.WithConnection(userId, c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "SELECT body FROM notes WHERE id = ?";
    cmd.Parameters.Add(noteId);
    return (string?)cmd.ExecuteScalar();
});
```

The key is any object with value equality (a `string`, a number, a `Guid`).
Checkouts without a key share one pool-wide key, so with constant writes
everything follows the latest writer — pass a key when you balance. While
the member is ejected or not ready, checkouts under its key go by the policy
instead. A statement is a
write unless its first keyword (after whitespace, comments and opening
parentheses) is `SELECT`, `SHOW`, `EXPLAIN`, `DESCRIBE`, `DESC`, `VALUES`,
`WITH` or `USE`.

**Watching it.** `pool.Members()` returns one `SkaidbPoolMember` per member,
sorted by endpoint:

```csharp
foreach (var m in pool.Members())
    Console.WriteLine($"{m.Endpoint} {m.Canonical} {m.State} {m.Latency?.TotalMilliseconds:F1} ms "
                      + $"in use {m.InUse} idle {m.Idle} seed {m.Seed}");
// db1:7000 10.0.0.1:7000 Ready 0.8 ms in use 2 idle 3 seed True
```

`State` is `Ready`, `Ejected`, `Resyncing`, `Unreachable` or `Unlisted`;
`Canonical` is the member's own name from `SHOW ENDPOINTS` (null until
discovered); `Latency` is null until the member has been measured.
Ejections and re-admissions are written to `System.Diagnostics.Trace` as
warnings. The rules are the same in every skaidb driver
([PROTOCOL.md §6.1](https://skaidb.org/docs/PROTOCOL.html#61-balancing-a-pool-across-members)).

## Semantics

- `maxsize` (default 10) bounds the connections kept **idle**, not the number
  checked out. A burst opens extras; on `Release()` the surplus is disposed.
- `Acquire()` takes an idle connection (to the member the policy picks, if it
  picks one) and checks `IsUsable` — which includes a zero-timeout poll of
  the socket, so a connection the server closed while it sat idle is
  disposed and the next one tried, and a fresh one is opened when none is
  left. An idle connection to a member that is no longer usable (ejected,
  not ready, gone) is disposed instead of handed out. A fresh connection
  runs the full `Open()` (endpoint walk, handshake, Hello, `USE`), dialing
  the picked member first and falling back to the other usable members in
  random order, then to the unusable ones.
- `Release()` puts the connection back if `IsUsable`, its member is usable
  and the idle set is below `maxsize`; otherwise it disposes it. A
  connection that hit a transport error, or whose `Stream()` was abandoned
  undisposed, is therefore never handed to another caller. A connection the
  pool did not hand out is disposed. Disposing a checked-out connection
  instead of releasing it is safe: it stops counting against its member.
- `WithConnection()` is `Acquire()` + the callback + `Release()` in a
  `finally`. Do not let a lazy `Stream()` enumerable escape the callback.
- `Dispose()` closes every idle connection and stops the background timer;
  later `Acquire()` calls throw `pool is closed`. Connections checked out at
  that moment are disposed when released.
- The background timer (re-probing ejected members, refreshing discovery)
  starts on first need, runs on the thread pool so it never keeps the
  process alive, and ends with `Dispose()`.
- The pool is thread-safe: the idle set and the member table are guarded by
  one lock. A connection handed out is the caller's alone until released.

## What a pool does not do

- It does not limit concurrency: it never blocks a caller waiting for a
  connection. Bound your own parallelism if the server needs it.
- It does not retry statements. A statement that fails with a transport
  error throws to the caller; the connection is disposed on release and the
  next `Acquire()` gives a healthy one.
- It does not health-check idle connections on a timer; the check happens on
  `Acquire()`.

The runnable [`examples/Pool`](../examples/Pool/Program.cs) drives a
least-latency pool from `Parallel.For` and prints what it sees per member.
