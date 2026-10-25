# Changelog

All notable changes to the skaidb C# / .NET driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Release dates are UTC.

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

[1.2.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.2.0
[1.1.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.1.0
[1.0.1]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.0.1
[1.0.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.0.0
