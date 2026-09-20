# skaidb — C# / .NET driver

[![CI](https://github.com/porcupin26/skaidb-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/porcupin26/skaidb-dotnet/actions/workflows/ci.yml)

The official [skaidb](https://skaidb.org/) driver for .NET. The API is
[ADO.NET](https://learn.microsoft.com/dotnet/framework/data/adonet/)-shaped —
`SkaidbConnection`, `SkaidbCommand`, `SkaidbDataReader` — so if you have used
`SqlConnection`/`SqlCommand`/`SqlDataReader` you already know it. **Pure BCL**:
no NuGet dependencies, one source file, targets `net8.0`.

- Speaks skaidb's binary protocol directly: SCRAM-SHA-256 authentication,
  server-side prepared statements with typed parameters, streaming result
  sets, one-round-trip batches, full type fidelity (`long`, `decimal`, `Guid`,
  `byte[]`, `DateTimeOffset`, arrays, documents).
- Multi-seed failover, transparent reconnect, TLS, connection pooling,
  stream subscriptions, multiple result sets.

Full documentation: this README, the [`docs/`](docs/) folder
([getting started](docs/getting-started.md) · [API reference](docs/api.md) ·
[types](docs/types.md) · [TLS](docs/tls.md) · [streaming](docs/streaming.md) ·
[pooling](docs/pooling.md)), runnable [`examples/`](examples/), the
[changelog](CHANGELOG.md), the
[wire protocol specification](https://skaidb.org/docs/PROTOCOL.html) and the
[skaidb documentation](https://skaidb.org/docs/).

## Install

The package is `Skaidb`. Until it is on NuGet.org (the publish workflow
pushes every tagged version there as soon as the repository has a NuGet
credential), installs come from the GitHub release: every release at
<https://github.com/porcupin26/skaidb-dotnet/releases> attaches
`Skaidb.X.Y.Z.nupkg` (and the `.snupkg` symbols package). Download it into
a folder and use that folder as a package source:

```sh
V=1.0.1
mkdir -p ~/nuget-local
curl -L -o ~/nuget-local/Skaidb.$V.nupkg \
  https://github.com/porcupin26/skaidb-dotnet/releases/download/v$V/Skaidb.$V.nupkg
dotnet nuget add source ~/nuget-local --name skaidb-local
dotnet add package Skaidb --version $V
```

Or reference the source directly, from a clone or a git submodule:

```sh
git clone https://github.com/porcupin26/skaidb-dotnet.git
dotnet add reference path/to/skaidb-dotnet/Skaidb.csproj
```

(Or just drop `Skaidb.cs` into your project — it has no dependencies.)

## Quick start

```csharp
using Skaidb;

using var conn = new SkaidbConnection(
    "Host=localhost;Port=7000;User=skaidb;Password=secret;Database=app");
conn.Open();   // TCP connect + SCRAM-SHA-256 handshake

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "CREATE TABLE users (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "INSERT INTO users (id, name, tags) VALUES (?, ?, ?)";
    cmd.Parameters.Add(1L);
    cmd.Parameters.Add("Ada");
    cmd.Parameters.Add(new[] { "admin" });     // arrays travel as typed parameters
    int affected = cmd.ExecuteNonQuery();      // 1
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name FROM users WHERE id = ?";
    cmd.Parameters.Add(1L);
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
        Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
}
```

`SkaidbConnection`, `SkaidbCommand` and `SkaidbDataReader` are `IDisposable`;
wrap them in `using`. Every error the driver raises is a `SkaidbException`.

## Connecting

```csharp
new SkaidbConnection("Host=db1;Port=7000;User=app;Password=secret;Database=app;Consistency=Quorum;Timeout=10");
new SkaidbConnection(host: "db1", port: 7000, user: "app", password: "secret", database: "app");
```

Connection strings are `;`-separated `Key=Value` pairs, keys case-insensitive:

| Key | Aliases | Default | Meaning |
|---|---|---|---|
| `Host` | `Server`, `Data Source` | `localhost` | Host to dial when `Seeds` is not given. |
| `Port` | | `7000` | Port to dial. |
| `Seeds` | | — | `db1:7000,db2:7000,db3:7000`; overrides `Host`/`Port`. Shuffled on every connect. |
| `User` | `Username`, `User Id`, `Uid` | `anonymous` | User name. |
| `Password` | `Pwd` | *(empty)* | Password. Empty means anonymous; mutual authentication is skipped. |
| `Database` | `Db` | — | Selected with `USE` after every (re)connect. |
| `Consistency` | | `Quorum` | `One`, `Quorum` or `All` (or `0`/`1`/`2`); the default for every command. |
| `Timeout` | `Connect Timeout` | `10` | Seconds; applies to the connect and to every socket read and write. |
| `Tls`, `TlsCa`, `TlsInsecure`, `TlsServerName` | | | See [TLS](#tls). |

`Open()` is idempotent. `IsOpen` says whether the handshake completed;
`IsUsable` additionally reports that the socket is alive, no stream is in
flight and no transport error is pending.

### Seeds and failover

skaidb is leaderless: every node accepts reads and writes, so a seed list is
only "somewhere to land". `Open()` shuffles the seeds and tries them in turn
until one connects; if none does it throws `no reachable endpoint in <list>:
<last error>`. The same walk runs again on reconnect (below), so a client
survives the node it was talking to going away.

### TLS

| Setting | Meaning |
|---|---|
| *(none)* | Plaintext. A server with `client_tls = required` refuses this. |
| `Tls=true` | TLS, server certificate verified against the system trust store. |
| `TlsCa=/path/ca.crt` | TLS, certificate verified against this PEM bundle. Implies `Tls`. |
| `TlsInsecure=true` | TLS with **no** certificate verification: encrypts, authenticates nothing. Development only. Implies `Tls`. |
| `TlsServerName` | SNI / name to verify (default `skaidb`). Must match a SAN on the server certificate, which is usually *not* the address you dialled — skaidb's own certificates carry `DNS:skaidb`. |

```csharp
new SkaidbConnection("Seeds=db1:7000,db2:7000;User=app;Password=secret;TlsCa=/etc/skaidb/skai-ca.crt");
```

More in [docs/tls.md](docs/tls.md).

### Consistency

How many replicas must acknowledge a write or be consulted for a read:
`One`, `Quorum` (the default) or `All`. Per connection, per command, or per
stream:

```csharp
conn.Consistency = SkaidbConsistency.All;              // default for new commands
cmd.Consistency  = SkaidbConsistency.One;              // this command only
foreach (var row in conn.Stream("SELECT ...", SkaidbConsistency.One)) { ... }
```

`ExecuteBatch` uses the command's level.

## Statements

### `SkaidbCommand`

```csharp
using var cmd = conn.CreateCommand();
cmd.CommandText = "UPDATE users SET name = ?, seen = ? WHERE id = ?";
cmd.Parameters.Add("Ada");
cmd.Parameters.Add(DateTimeOffset.UtcNow);
cmd.Parameters.Add(1L);
int n = cmd.ExecuteNonQuery();     // rows affected; 0 for DDL and SELECT
```

| Method | Returns |
|---|---|
| `ExecuteReader()` | a `SkaidbDataReader` over the rows (empty, `FieldCount` 0, for a non-row statement) |
| `ExecuteNonQuery()` | rows affected for INSERT/UPDATE/DELETE, `0` otherwise |
| `ExecuteScalar()` | the first column of the first row, or `null` when there is none (a NULL cell is also `null`) |
| `ExecuteBatch(rows)` | total rows affected, one round-trip; see below |

Statements on one connection are **serialized**: one request/response is in
flight at a time. A second thread running a statement on the same connection
waits for the lock, or — while a stream is open — is refused. Use a
[pool](#pooling) for concurrency.

### Parameters and prepared statements

Placeholders are `?`, bound positionally from `cmd.Parameters` in order. A
`?` inside a string literal is left alone.

With parameters the driver **prepares the statement on the server** (once per
distinct SQL text per connection, cached up to 240 entries) and sends the
values as **typed binary** — no string interpolation, no injection surface,
and .NET arrays and dictionaries travel as skaidb `Array` and `Document`
values, which have no SQL literal form. The statement's parameter count must
match what you add (`statement expects N parameters, got M` is thrown before
anything is sent).

There is no separate prepare/execute API: preparing is automatic. Statements
the server declines to prepare (DDL, `USE` and other session statements) fall
back to safe client-side quoting of the same `?` parameters, so every
statement kind accepts parameters — except that arrays and documents cannot
be rendered as text. Prepared ids are scoped to a connection and the cache is
dropped on reconnect.

### `ExecuteBatch(rows)` — many rows, one round-trip

```csharp
var rows = new List<IReadOnlyList<object?>>
{
    new object?[] { 1L, new[] { "a" } },
    new object?[] { 2L, Array.Empty<string>() },
    new object?[] { 3L, new[] { "b", "c" } },
};
cmd.CommandText = "INSERT INTO t (id, tags) VALUES (?, ?)";
long n = cmd.ExecuteBatch(rows);   // 3
```

Runs a prepared statement once per row in a single request and returns the
**total** affected count. Rows **autocommit individually**: on the first
failing row the server answers with an error naming the row index and how
many rows applied before it, and those earlier rows stay applied — so use
idempotent statements. Only preparable statements (`SELECT`/`INSERT`/
`UPDATE`/`DELETE`) can be batched; the whole request must fit one 64 MiB
frame. An empty list returns 0 without a round-trip.

### Multiple result sets

A `CALL` of a procedure whose body runs `EMIT <select>` answers with every
emitted set plus the call's final result. The reader starts on the first
set; `NextResult()` advances:

```csharp
cmd.CommandText = "CALL report()";
using var r = cmd.ExecuteReader();
do
{
    while (r.Read()) { ... }
} while (r.NextResult());
```

### Transactions

Every statement autocommits; the driver adds no transaction API of its own.
Whatever transaction statements your server version supports are plain SQL
sent through a command on one connection — but on a **cluster** `BEGIN` is
not supported, and every statement is applied on its own. `ExecuteBatch` rows
autocommit one by one, as described above.

## Streaming large results — `conn.Stream(sql, consistency?)`

`ExecuteReader()` buffers the whole result; a large scan is also bounded by
the server's scan budgets. `Stream()` asks the server to deliver the rows in
chunks and yields them one at a time, holding one chunk in memory:

```csharp
foreach (object?[] row in conn.Stream("SELECT id, v FROM readings"))
    Process((long)row[0]!, (double)row[1]!);
Console.WriteLine(string.Join(",", conn.StreamColumns));   // set once the header arrives
```

It takes **no parameters** (the streaming opcode carries SQL text only). A
non-row statement streamed this way yields nothing. An error before any row
is an ordinary statement error; an error partway through (a node dying
mid-scan, a scan budget tripping) is thrown after the rows already yielded,
which are valid.

On a cluster, name the columns: a bare `SELECT *` is only executed page by
page at consistency `One`; a named column list streams at any level.

### The abandon rule

**The connection is busy for the whole stream, and disposing the enumerator
is what frees it.** `foreach` disposes on every exit — `break`, `return`, an
exception. On an early exit the driver reads the rest of the result out (up
to 64 frames) so the socket is left at a request boundary; past that it drops
the connection instead, and the next statement transparently reconnects.
Either way the next statement reads its own reply.

A hand-rolled `GetEnumerator()` that is merely dropped half-read is disposed
by nobody — .NET runs no iterator `finally` at collection time — and that
connection stays busy for good: `IsUsable` is false, a pool discards it, and
every statement on it throws `connection is busy streaming`. Always enumerate
to the end or dispose:

```csharp
using var it = conn.Stream("SELECT ...").GetEnumerator();
if (it.MoveNext()) { var first = it.Current; ... }
// Dispose() releases the connection
```

More in [docs/streaming.md](docs/streaming.md).

## Following a stream — `conn.Subscribe(name, after?, pollMs?)`

For tables with a `CREATE STREAM`, `Subscribe()` yields the stream's events as
they arrive, forever:

```csharp
foreach (var ev in conn.Subscribe("big_orders", after: lastSeenId, pollMs: 500))
{
    // ev = StreamEvent(Id, Op, Key, Ts, Doc)
    Save(ev.Id);      // pass it back as `after` to resume exactly here
}
```

It polls the stream's log (`_stream_<name>`) with a keyset cursor, 500 events
per page; `Id` is the position — an opaque **string** that sorts in log order
(`"00000001789857620741-0000000000-0902800000000000000100"`), not a number,
so `after` takes a saved `Id` or `null`. To stop, `break` out of the loop (or
dispose the enumerator); the connection is idle between polls, so a stop takes
effect within `pollMs`. For push delivery subscribe to `$stream/<db>/<name>`
with any MQTT client instead — the events are identical.

## Pooling

```csharp
using var pool = new SkaidbConnectionPool(
    "Seeds=h1:7000,h2:7000;User=app;Password=secret;Database=app", maxsize: 8);

long n = pool.WithConnection(c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "SELECT count(*) FROM t";
    return (long)cmd.ExecuteScalar()!;
});

var conn = pool.Acquire();
try { ... } finally { pool.Release(conn); }
```

`maxsize` (default 10) bounds the connections kept **idle**, not the number
checked out: a burst opens extras and the surplus is disposed on release.
`Acquire()` validates an idle connection before handing it out and discards
one the server closed meanwhile; `Release()` disposes a broken connection
instead of pooling it. Pooled connections inherit seeds, TLS and the session
database from the connection string. Do not return a `Stream()` enumerable
out of `WithConnection`: it is lazy, and the connection would go back to the
pool before the first row is read. More in [docs/pooling.md](docs/pooling.md).

## Reconnect and errors

Every error the driver raises is a `SkaidbException`.

- **Statement errors** (`SELECT 1 FROM no_such_table` →
  `table "no_such_table" does not exist`, a parse error, a constraint
  violation) throw; the connection stays usable. Note that an unknown bare
  column name in a select list is *not* an error on the server: `SELECT nope
  FROM t` returns a column `nope` of NULLs.
- **Transport errors** throw `read failed: …` / `write failed: …` /
  `connection closed by server` from the in-flight statement and mark the
  connection *broken*, not closed. The next statement **re-dials** (through
  the seed walk), re-authenticates, re-sends `USE <database>`, re-sends the
  Hello frame and clears the prepared-statement cache. Because the failed
  statement *may* have executed, the driver never retries it for you: retry a
  write only if it is idempotent.
- A connection the server closed while it sat **idle** is detected before
  the next statement is sent, so that statement simply re-dials and runs —
  nothing was in flight, so nothing is ambiguous.
- **Protocol desync** (an unexpected frame): the driver drops the connection
  (`IsUsable` → false) rather than hand the next caller a misaligned reply,
  and the next statement reconnects.
- `Close()`/`Dispose()` are terminal: a disposed connection never reconnects
  (`ObjectDisposedException`).

Common messages: `connect failed: …`, `connect to … timed out`, `no reachable
endpoint in …`, `authentication denied: …`, `server signature mismatch
(mutual auth failed)`, `server does not support streaming: …`, `statement
expects N parameters, got M`, `more placeholders than parameters`, `cannot
bind value of type …`, `cannot bind NaN/Infinity`, `connection is busy
streaming: …`, `unknown connection string key: …`.

## Type mapping

| skaidb | .NET (results, `GetValue`) | Bind (parameters) |
|---|---|---|
| Null | `DBNull.Value` (`IsDBNull(i)` true) | `null`, `DBNull.Value` |
| Bool | `bool` | `bool` |
| Int (i64) | `long` | `sbyte`…`long`, `ulong`/`BigInteger` when they fit |
| Float | `double` | `float`, `double` (NaN/Infinity refused) |
| Decimal | `decimal`; a `string` when it exceeds 96 bits or scale 28 | `decimal`, out-of-range `ulong`/`BigInteger` (as scale 0) |
| String | `string` | `string`, `char` |
| Bytes | `byte[]` | `byte[]` |
| Uuid | `Guid` | `Guid` |
| Timestamp | `DateTimeOffset` (UTC, millisecond precision) | `DateTimeOffset`, `DateTime` (Unspecified kind is taken as UTC) |
| Array | `object?[]` (nested values follow this table) | any `IEnumerable` (except `string`/`byte[]`/dictionaries) |
| Document | `Dictionary<string, object?>`, keys in the server's order (sorted by key, not the order you inserted them) | any `IDictionary` with `string` keys |

Typed getters: `GetBoolean`, `GetInt32`, `GetInt64`, `GetDouble`,
`GetDecimal`, `GetString`, `GetGuid`, `GetDateTimeOffset`, `GetBytes`,
`GetValue`, `this[int]`, `this[string]`; they throw `SkaidbException` on a
NULL cell. In the client-side fallback (unpreparable statements) a
`DateTimeOffset` renders as its epoch milliseconds, a `byte[]` as a hex
string, and arrays/documents cannot be bound at all. Details in
[docs/types.md](docs/types.md).

## Client identification

After authenticating, the driver sends a Hello frame that fills the server's
`drivers` table: `client_name` `dotnet`, `client_version` = this package's
version (`SkaidbConnection.DriverVersion`, read from the assembly's
informational version, which the build derives from the single `<Version>` in
`Skaidb.csproj`). Servers that predate the frame answer with an error the
driver ignores.

```sql
SELECT client_name, client_version FROM drivers;   -- dotnet | 1.0.1
```

## Compatibility

- **.NET** 8.0 or newer. No build step beyond the SDK, no native code.
- **Server**: any skaidb. Prepared statements need server ≥ 0.17.0 (older
  servers get the client-side fallback automatically), `ExecuteBatch` ≥
  0.87.0, `Stream()` a server with the streaming opcode (an older one answers
  `server does not support streaming`), multiple result sets a server with
  `EMIT`, the `drivers` row a server ≥ 0.203.0.
- **Wire protocol**: <https://skaidb.org/docs/PROTOCOL.html>.

## Development

```sh
dotnet build Skaidb.sln -c Release
dotnet test tests/Skaidb.Tests          # xunit, in-process fake server, no skaidb needed (~1 s)
dotnet pack Skaidb.csproj -c Release    # the NuGet package
SKAIDB_LIVE=127.0.0.1:7000 SKAIDB_USER=u SKAIDB_PASSWORD=p SKAIDB_DATABASE=app \
  dotnet run --project tests/Live       # end to end against a real node (skips without SKAIDB_LIVE)
dotnet run --project examples/Basic -- host 7000 user password database
```

CI builds and tests on .NET 8 for every push and pull request and checks that
the Hello version equals the package version. Tagging `vX.Y.Z` runs the
publish workflow (`.github/workflows/publish.yml`): it refuses a tag that
differs from `<Version>`, builds and tests, packs `Skaidb.X.Y.Z.nupkg` and
the `Skaidb.X.Y.Z.snupkg` symbols package, pushes both to NuGet.org and
creates a GitHub Release with both attached. The NuGet.org credential is
either the `NUGET_API_KEY` repository secret (an API key from nuget.org) or
the `NUGET_USER` secret with a Trusted Publishing policy on nuget.org for
`publish.yml` (a short-lived key from the job's OIDC token, nothing to
rotate); with neither, the push is skipped with a notice and the release
asset is the install channel. The package declares its license as the packed
`LICENSE` file rather than an SPDX expression: nuget.org only accepts OSI- or
FSF-approved licenses in an expression, and SSPL-1.0 is neither.

## License

[SSPL-1.0](LICENSE).
