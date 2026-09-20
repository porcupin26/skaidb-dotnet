# Changelog

All notable changes to the skaidb C# / .NET driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Release dates are UTC.

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

[1.0.0]: https://github.com/porcupin26/skaidb-dotnet/releases/tag/v1.0.0
