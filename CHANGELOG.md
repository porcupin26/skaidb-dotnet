# Changelog

All notable changes to the skaidb C# / .NET driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Release dates are UTC.

## [1.5.1] - 2026-11-16

### Fixed
- **A database entered with a `USE` statement is entered again after a
  re-dial.** The driver re-entered the database of the connection string,
  or of `ChangeDatabase()`, on every dial, but not one entered with a `USE`
  sent as an ordinary statement. After a transparent re-dial — the member
  restarted while the connection sat idle — the session was in the default
  database, and the statements that followed ran against the tables of the
  same names there: a read came back from the wrong table and a write went
  into it, with no error. A statement the server has accepted that is
  exactly `USE [DATABASE] <name>` (the name bare or in double quotes, an
  optional `;` behind it) now sets the connection's `Database`, which every
  dial enters before anything else is sent, on a pooled connection too. A
  `USE` written with `--` comments is read the same way, as the server
  reads it: `USE app -- the app database` is noted, and `USE "a--b"` names
  the database `a--b`. If the database cannot be entered on the new
  connection the statement fails with that error and is not sent.
- **A borrower's `USE` no longer reaches the next borrower of a pool with
  a `Database`.** A connection whose borrower ran `USE other`, or
  `ChangeDatabase()`, went back to the pool in that database, and the next
  borrower's statements ran there. `Release()` now sends `USE` for the
  database of the pool's connection string before the connection goes
  back, and disposes it if that fails. A pool whose connection string
  names no `Database` is unchanged: there a borrower's `USE` stays on the
  connection.
- **A connection's prepared statements no longer fill the server's
  table.** A server holds at most 256 prepared statements per connection.
  The driver cached the first 240 distinct parameterised texts of a
  connection; a text past those was prepared again at every execution and
  never closed, so the 257th prepare was refused, and that statement and
  every parameterised statement outside the cache after it threw `too many
  prepared statements (max 256); close some first` until the connection
  was re-dialed. The connection now keeps up to 240 prepared statements
  and, when another is needed, closes the one used longest ago on the
  server; it is prepared again if its text comes back. Any number of
  distinct parameterised statements can run on one connection, each bound
  with typed values.
- **An `Open()` that fails at its `USE` leaves the connection closed.**
  When the server refused the `USE` of the session database, `Open()`
  threw but `IsOpen` stayed true and `State` was `Open`, and a second
  `Open()` returned at once without dialing.
- **A connection marked broken closes its socket at once.** A transport
  failure left the dead socket open until the connection's next statement
  re-dialed, or `Close()`. When the server was still on the other end — a
  read that timed out, a stream abandoned with more than 64 frames left —
  it kept the session until then, and with it the transaction a `BEGIN`
  had opened and the rows that transaction had claimed.
  `conn.Transaction()` could not end it either: its `ROLLBACK` is refused
  on a broken connection. The socket is now closed where the connection is
  marked broken; `IsOpen` stays true and the next statement re-dials, as
  before. A read that times out in the middle of a stream is also reported
  after one timeout, not two.
- **A stream the body of `Transaction()` leaves open no longer leaves its
  transaction open.** A `Stream()` enumerator the body started and neither
  ran out nor disposed kept the connection busy, so the helper's `COMMIT`
  (or `ROLLBACK`) was refused client-side with `connection is busy
  streaming`: the helper threw, and the transaction stayed open on a
  working socket. Statements run on that connection once the stream was
  disposed ran inside it — unseen by other connections, and undone by the
  next `ROLLBACK` or disconnect. The helper now closes the socket, so the
  server drops the transaction. It throws what it threw before (the body's
  exception, else the busy-streaming error), and the connection re-dials
  on its next statement once the stream has been disposed.
- **`Open()` inside the body of `Transaction()` is refused.** A body that
  caught a lost connection's error, called `Close()` and `Open()` and
  carried on got a connection that never saw the `BEGIN`: its remaining
  statements were applied one by one, in autocommit, and the helper sent
  its `COMMIT` there. `Open()` now throws `connection lost inside a
  transaction: …` until the helper has returned.
- **A re-dial that fails is tried again.** When a statement found its
  connection lost and the re-dial failed too — the server still
  restarting — the connection was left closed: every later statement and
  every `Transaction()` threw `connection is not open`, also once the
  server was back, until the application called `Open()`. The connection
  now stays open and broken, so each statement dials again and the first
  one after the server is back runs.
- **A connection opened past a seed that dropped the handshake is
  usable.** A seed that accepts the socket and closes it (a node at its
  connection ceiling) left the connection the next seed gave marked
  broken: `IsUsable` was false right after `Open()`, the first statement
  dialed a second time, and a pool threw the new connection away and
  dialed again.
- **A frame over the 64 MiB limit drops the connection.** `frame too
  large` was thrown with the frame's payload unread on a connection that
  stayed usable, so the next statement read its reply out of that payload.
  The connection is now broken, and the next statement re-dials.

## [1.5.0] - 2026-11-15

### Added
- **`SkaidbTransactionConflictException`**, a `SkaidbException`: a server
  error whose message starts with `transaction conflict:` — a standalone
  server (0.346 and later) refusing a `COMMIT` because another writer
  changed a row the transaction writes after its `BEGIN`. Every path that
  surfaces a server error throws it (commands, prepared statements,
  `ExecuteBatch`, `DbBatch`, `Stream()`), with the server's message
  unchanged; only the prefix is matched. It is a statement error: existing
  `catch (SkaidbException)` blocks still see it, the connection stays
  usable, and a pool neither counts it against the member nor discards the
  connection.
- **`conn.Transaction(body, attempts = 3, begin = "BEGIN")`** runs `body`
  inside a transaction on the connection and returns its value (an `Action`
  overload returns nothing). It sends `begin`, calls the body with the
  connection, and sends `COMMIT`; when the commit is refused with a conflict
  the whole transaction runs again from `begin`, at once, up to `attempts`
  runs in all — so the body must be safe to run more than once. An
  exception out of the body sends `ROLLBACK` (whatever that answers is
  ignored) and is rethrown unchanged, without a retry; a refused `begin` is
  thrown before the body runs; any other `COMMIT` error is thrown with no
  retry and no `ROLLBACK`; `attempts` below 1 is an
  `ArgumentOutOfRangeException` before anything is sent. `begin` is sent as
  given, so it takes the cluster forms (`BEGIN ATOMIC`, `BEGIN ATOMIC
  PARTITION (col = lit)`). The body is synchronous: one that returns a task
  (an `async` lambda) is refused with an `ArgumentException` before anything
  is sent. While the transaction is open the connection does not re-dial: a
  statement that finds the socket closed throws `connection lost inside a
  transaction: …` instead of carrying on in autocommit on a new connection.
- **`pool.Transaction(body, attempts = 3, begin = "BEGIN")`**: the same on
  one checked-out connection, held for every attempt and returned
  afterwards.

### Changed
- `BeginTransaction()` still throws `NotSupportedException` (there is no
  `DbTransaction` object), and so does setting a non-null `Transaction` on a
  command or batch; their messages now name `SkaidbConnection.Transaction()`.

## [1.4.1] - 2026-10-31

### Fixed
- One slow round trip at the start can no longer get a healthy member
  ejected for latency. A member's first round trip set its average
  outright, so a statement that stalled in the client — a CPU-bound
  application holding the thread while it starts — put seconds into it;
  ten round trips later the member was compared with the others, and the
  average had barely decayed: ejected (`latency 4153.7 ms vs median
  0.2 ms`) while it answered in milliseconds. The fastest of a member's
  first three round trips now seeds its average (a round trip is only ever
  lengthened), and later ones move it as before. Until then the member
  counts as not measured: `Members()` reports its `Latency` as null.
- A latency ejection's `System.Diagnostics.Trace` warning names the slowest
  of the member's last 10 round trips alongside its average and the
  median, which tells a slow member from one slow statement.

## [1.4.0] - 2026-10-29

### Fixed
- A streamed `CALL` whose procedure body `EMIT`s result sets returns them.
  The server answers such a call with every set in one `ResultSets` frame
  (tag 8) even to a stream request, and `conn.Stream()`, with or without
  parameters, threw `unexpected response tag 8 to stream request` and
  marked the connection broken. The frame is now decoded by the code
  `ExecuteReader()` uses: the enumeration yields the first set's rows
  (`StreamColumns` holds its columns), and the new
  `conn.StreamResultSets` is the reader `ExecuteReader()` would return for
  the statement, on the first set with `NextResult()` walking the rest
  (null for every other statement). The exchange is complete in that one
  frame, so the connection is handed back before the first row is yielded
  and stays usable.

## [1.3.0] - 2026-10-29

### Added
- **Parameterised streaming**: `conn.Stream(sql, parameters, consistency?)`
  binds the statement's `?` placeholders in order, typed — the values
  `cmd.Parameters` takes — over `OP_QUERY_STREAM_PARAMS` (server ≥ 0.330),
  so a range-bounded export no longer needs its literals written into the
  SQL: `conn.Stream("SELECT … WHERE ts >= ? AND ts < ?", [start, end])`.
  `parameters` is an `IReadOnlyList<object?>` like an `ExecuteBatch` row (an
  array, a list, a collection expression). An older server's refusal is
  thrown as `server does not support parameterised streaming (skaidb >=
  0.330): …` and leaves the connection usable. Without parameters (none,
  null or an empty list) the request is the original `OP_QUERY_STREAM`,
  byte for byte, so it works against every server. `Stream(sql)` and
  `Stream(sql, consistency)` are unchanged, binary included: every call form
  of them, `Stream(sql, null)` too, binds as before, and a consistency is
  never taken for a parameter. The conformance suite's `stream_params` case
  runs through it.

## [1.2.0] - 2026-10-25

### Added
- **Pool load balancing across members** (the rules every skaidb driver
  shares, PROTOCOL.md §6.1): `new SkaidbConnectionPool(connectionString,
  new SkaidbPoolOptions { ... })` with
  `LoadBalancing = SkaidbLoadBalancing.RoundRobin` / `LeastLatency` (power of
  two choices on the round-trip average × checked-out connections; a member
  not measured in the last 2 s is tried again; `Random` stays the default),
  `IdleOrder = SkaidbIdleOrder.Fifo`, health ejection (`EjectErrors`
  consecutive transport failures, or a latency average far above the other
  members': `EjectLatencyFactor`, `EjectLatencyFloor`) with a background
  probe that re-admits the member (`EjectBackoff` doubling to
  `EjectBackoffMax`), `Discover = true` (members from `SHOW ENDPOINTS`,
  server ≥ 0.327: joiners added, leavers dropped, resyncing/unreachable
  members skipped and re-checked every 5 s), and `ReadYourWrites =
  <TimeSpan>` with `Acquire(affinity)` / `WithConnection(affinity, work)` —
  reads follow the member that took the key's last write.
  `pool.Members()` reports each member's endpoint, discovered name, state,
  latency and connections (`SkaidbPoolMember`). The
  `(connectionString, maxsize)` constructor keeps working.

### Changed
- `Open()`, and the re-dial after a transport error, move on to the next
  seed when a node accepts the TCP connection but drops the handshake (a
  node at its connection ceiling, or one going down) instead of failing; a
  refused login still fails at once.
- A pooled connection re-dials in the pool's order (usable members first,
  the one it lost last).
- `SkaidbConnectionPool` reads its connection string when constructed, so an
  invalid one throws from the constructor rather than from the first
  `Acquire()`. `Release()` disposes a connection the pool did not hand out
  instead of keeping it.

## [1.1.0] - 2026-09-26

### Added
- A genuine ADO.NET provider. `SkaidbConnection` now derives from
  `DbConnection`, `SkaidbCommand` from `DbCommand`, `SkaidbDataReader` from
  `DbDataReader` and `SkaidbException` from `DbException`; new
  `SkaidbParameter`/`SkaidbParameterCollection` (`DbParameter`),
  `SkaidbConnectionStringBuilder` (`DbConnectionStringBuilder`),
  `SkaidbBatch`/`SkaidbBatchCommand` (`DbBatch`, merging runs of one
  parameterised statement into a single `OP_EXECUTE_BATCH`) and
  `SkaidbFactory` (`DbProviderFactory`, static `Instance`, invariant name
  `Skaidb` in `SkaidbFactory.ProviderInvariantName`). Register with
  `DbProviderFactories.RegisterFactory("Skaidb", SkaidbFactory.Instance)`.
  Dapper works as-is.
- `@name` and `$N` placeholders, rewritten to the server's `?` before
  preparing; `Parameters.AddWithValue(name, value)`.
  `CommandType.StoredProcedure` runs `CALL name(?, …)`.
- Reader: `GetFieldValue<T>`, `GetSchemaTable()`, `GetFieldType`,
  `GetDataTypeName`, `RecordsAffected`, `HasRows`, `GetDateTime`,
  `GetValues`, chunked `GetBytes`/`GetChars`, `CommandBehavior.CloseConnection`.
- Connection strings: `Host` takes a comma list of `host[:port]` seeds
  (`[v6]:port` too), values may be quoted, `Username` is the canonical key
  name. `SkaidbConnection()` plus a settable `ConnectionString`,
  `State`/`StateChange`, `DataSource`, `ChangeDatabase()`.
- Certificate login (wire mechanism EXTERNAL): `TlsClientCert` /
  `TlsClientKey` present a PEM client certificate, and
  `AuthMechanism=Certificate` makes it the login. Without an explicit
  `Username` the AuthStart carries an empty name, so the certificate's CN
  decides (the `anonymous` default is never sent).
- `SkaidbConnection.StreamAffected`: the affected-row count of a streamed
  mutation.
- The shared wire-protocol conformance suite (`conformance/vectors.json`,
  vendored from https://skaidb.org/conformance/vectors.json) runs in the
  unit tests; CI fails when the vendored copy differs from the published one.

### Changed
- `Close()` releases the socket but no longer disposes the connection:
  `Open()` dials again. `Dispose()` stays final.
- `SkaidbCommand.Parameters` is a `SkaidbParameterCollection` (was
  `List<object?>`); `Parameters.Add(value)` still adds a positional value.
  Code that used other `List<T>` members, or that was compiled against 1.0.x,
  needs a rebuild.
- `BeginTransaction()` throws `NotSupportedException` (the protocol has no
  transactions; every statement autocommits).
- An invalid `Port` or `Timeout` in a connection string throws
  `SkaidbException` (was a raw `FormatException`); `Port` must be 1-65535 and
  `Timeout` positive.

### Fixed
- Found by the conformance suite: a streamed mutation's affected-row count
  was lost (`Stream()` yielded nothing and exposed no count), and
  `StreamColumns` kept the previous stream's columns after a non-row or
  failed stream.
- A `Host` value carrying a port (`Host=db1:7001`) dialled `db1:7001:7000`.

## [1.0.1] - 2026-09-20

Release automation: published from GitHub Actions. No driver code changed.

### Changed
- The package declares its license as the packed `LICENSE` file
  (`PackageLicenseFile`) instead of the `SSPL-1.0` SPDX expression:
  NuGet.org accepts only OSI- or FSF-approved licenses in an expression and
  rejects the push otherwise. The license itself is unchanged.
- The package ships a symbols package, `Skaidb.X.Y.Z.snupkg` (portable PDB
  with Source Link), next to the `.nupkg`; the publish workflow attaches both
  to the GitHub Release and pushes both to NuGet.org, and CI asserts both
  are produced.
- The publish workflow pushes to NuGet.org with either the `NUGET_API_KEY`
  secret or NuGet Trusted Publishing (`NUGET_USER` secret plus a policy on
  nuget.org for `publish.yml`); with neither it skips the push with a notice
  and stays green. The release body now says which install path applies and
  carries this changelog's section for the version.

## [1.0.0] - 2026-09-20

First release as a standalone repository
(`github.com/porcupin26/skaidb-dotnet`), carrying its full history over from
the skaidb monorepo. This is also the first release of the driver that was
run against a live server: the fixes below are what that found.

### Added
- `SkaidbConnection`: SCRAM-SHA-256 handshake, `?`-style parameters through
  server-side prepared statements with typed bindings, `ExecuteBatch()`,
  `Stream()` with the abandon/drain rule, `Subscribe()` over stream logs,
  multi-seed failover, transparent reconnect, TLS (`Tls`, `TlsCa`,
  `TlsInsecure`, `TlsServerName`), per-connection `Database`, and
  per-connection, per-command or per-stream consistency (`One`/`Quorum`/`All`).
- `SkaidbConnectionPool`: a bounded idle pool with
  `Acquire()`/`Release()`/`WithConnection()`.
- Multiple result sets from a `CALL` whose body `EMIT`s, walked with
  `SkaidbDataReader.NextResult()`.
- `SkaidbConnection.DriverVersion` and `DriverName`: the version the Hello
  frame reports is read from the assembly's informational version, which the
  build derives from the single `<Version>` in `Skaidb.csproj` — never a
  literal. A unit test and a CI step assert the two agree.
- `IsUsable` now also detects a socket the server closed while the
  connection sat idle (a zero-timeout poll), so a pool discards such a
  connection on `Acquire()` instead of handing it out; and a statement on
  such a connection re-dials *before* sending anything, rather than failing
  once and reconnecting on the next call.
- xunit test suite with an in-process fake server: framing, the SCRAM
  handshake (including a forged server signature), the value codec for every
  type, client-side binding, connection strings, prepare/execute with typed
  parameters, batches, streaming chunks and the abandon/drain rule, error
  responses, failover, reconnect, the pool, `Subscribe()`, and the
  Hello-version-equals-package-version check.
- A live end-to-end test (`tests/Live`) that runs against a real node when
  `SKAIDB_LIVE` is set and is skipped otherwise.
- CI on .NET 8 for pushes, pull requests and tags; a publish workflow on
  `v*` tags that gates on tag == `<Version>`, packs `Skaidb.X.Y.Z.nupkg`,
  attaches it to a GitHub Release and pushes to NuGet.org only when
  `NUGET_API_KEY` exists.
- Documentation: README plus `docs/` (getting started, API reference, types,
  TLS, streaming, pooling), and runnable `examples/` (Basic, PreparedBatch,
  Stream, Tls, Pool, Subscribe).

### Fixed
- README: the in-page `[pool](#pooling)` link in the Statements section
  pointed nowhere — the heading was ``## Pooling — `SkaidbConnectionPool` ``,
  whose generated anchor differs per renderer (GitHub: `pooling--skaidbconnectionpool`).
  The heading is plain `## Pooling` now, so the link resolves on GitHub and
  in the README packed into the NuGet package alike.
- Documentation: a document's keys come back in the server's sorted order,
  not the order they were bound in (the server canonicalises documents on
  write; the driver keeps the wire order). The README and `docs/types.md`
  claimed "insertion order kept"; the live test now asserts the sorted order.
- Documentation: the statement-error example was `SELECT nope` →
  `no such column …`, which the server does not raise (an unknown bare column
  yields a NULL column). The example is now a missing table
  (`table "…" does not exist`), which is what the live test exercises.
- Binding a `decimal` parameter to a prepared statement threw `cannot bind
  value of type Decimal`: the typed encoder had no Decimal case. It now
  encodes the exact (mantissa, scale) pair, as does a `BigInteger` or `ulong`
  outside the 64-bit range (scale 0). `char` binds as a one-character string.
- A prepare that failed with a real SQL error (a parse error, an unknown
  column) was treated as "statement kind cannot be prepared" and retried as
  text, where an array or document parameter then failed with `cannot bind
  value of type Object[]` and hid the real message. Only the server's
  "cannot be prepared" refusal (or an old server's "unknown opcode") falls
  back now; any other prepare error is thrown as-is.
- A `DateTime` of `Unspecified` kind was converted from local time in the
  typed encoder but taken as UTC in the client-side binder; both take it as
  UTC now.
- `connect failed: One or more errors occurred (…)`: the `AggregateException`
  from the connect wait is unwrapped so the message names the real cause.
- Each request is written as one buffer (header + payload) instead of two,
  which with `NoDelay` meant two TCP segments per request.

### Changed
- Package version series restarts at 1.0.0; the library targets `net8.0`.
- Package metadata points at <https://github.com/porcupin26/skaidb-dotnet>;
  license SSPL-1.0; the README is packed into the package.
- The example moved to `examples/Basic`; the library, tests and examples are
  in `Skaidb.sln`.

[1.5.1]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.5.1
[1.5.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.5.0
[1.4.1]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.4.1
[1.4.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.4.0
[1.3.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.3.0
[1.2.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.2.0
[1.1.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.1.0
[1.0.1]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.0.1
[1.0.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.0.0
