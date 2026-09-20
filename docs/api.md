# API reference

```csharp
using Skaidb;
```

Everything lives in the `Skaidb` namespace: `SkaidbConnection`,
`SkaidbCommand`, `SkaidbDataReader`, `SkaidbConnectionPool`,
`SkaidbConsistency`, `SkaidbException`, `UnpreparableException`.

## `SkaidbConnection`

### Constructors

```csharp
new SkaidbConnection(string connectionString);
new SkaidbConnection(string host = "localhost", int port = 7000, string user = "anonymous",
                     string password = "", SkaidbConsistency consistency = SkaidbConsistency.Quorum,
                     string database = "", bool tls = false, string tlsCa = "",
                     bool tlsInsecure = false, string tlsServerName = "skaidb");
```

Connection string keys (`;`-separated `Key=Value`, case-insensitive; an
unknown key throws `SkaidbException`):

| Key | Aliases | Default | Meaning |
|---|---|---|---|
| `Host` | `Server`, `DataSource`, `Data Source` | `localhost` | Host to dial when `Seeds` is absent. |
| `Port` | | `7000` | Port to dial. A seed given without a port gets `7000`. |
| `Seeds` | | — | Comma-separated `host:port` list; overrides `Host`/`Port`. Shuffled on every connect. |
| `User` | `Username`, `UserId`, `User Id`, `Uid` | `anonymous` | |
| `Password` | `Pwd` | `""` | Empty skips mutual authentication (anonymous). |
| `Database` | `Db` | `""` | `USE "<database>"` after every (re)connect. |
| `Consistency` | | `Quorum` | `One`/`Quorum`/`All` or `0`/`1`/`2`, case-insensitive. |
| `Timeout` | `ConnectTimeout`, `Connect Timeout` | `10` | Seconds (decimals allowed). Connect, and every socket read/write. |
| `Tls` | | `false` | `true`/`1`: TLS with the system trust store. |
| `TlsCa` | `Tls Ca` | `""` | PEM bundle path; implies `Tls`. |
| `TlsInsecure` | `Tls Insecure` | `false` | No certificate verification; implies `Tls`. |
| `TlsServerName` | `Tls Server Name` | `skaidb` | SNI and verification name. |

The constructor only stores configuration; nothing is dialled until `Open()`.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `Host`, `Port`, `User` | | As configured. |
| `Seeds` | `IReadOnlyList<string>` | The endpoints tried, `host:port`. |
| `Database`, `Tls`, `TlsCa`, `TlsInsecure`, `TlsServerName` | | As configured. |
| `Consistency` | `SkaidbConsistency` | Read/write. Default for commands created afterwards. |
| `Timeout` | `TimeSpan` | Read/write; set before `Open()`. |
| `IsOpen` | `bool` | The handshake completed and `Dispose()` was not called. |
| `IsUsable` | `bool` | `IsOpen`, and: no transport error pending, no `Stream()` in flight, and the socket not closed by the peer. What a pool checks. |
| `StreamColumns` | `IReadOnlyList<string>` | Column names of the most recent `Stream()` call, set once its header arrives. |
| `DriverName` | `const string` | `"dotnet"`. |
| `DriverVersion` | `static string` | The package version, from the assembly's informational version. |

### `Open()`

Shuffles the seeds and tries each (TCP connect, then TLS if configured)
until one connects; runs the SCRAM-SHA-256 handshake; sends the Hello frame
(driver name `dotnet`, version `DriverVersion`; ignored by servers that
predate it); runs `USE "<database>"` if `Database` was given. Idempotent:
a second call on an open connection returns at once. Throws
`SkaidbException` — `no reachable endpoint in <seeds>: <last error>`,
`authentication denied: …`, `server signature mismatch (mutual auth failed)`,
`connect failed: …` — and `ObjectDisposedException` after `Dispose()`.

### `CreateCommand()` → `SkaidbCommand`

A command bound to this connection, with `Consistency` copied from it.

### `Stream(string sql, SkaidbConsistency? consistency = null)` → `IEnumerable<object?[]>`

Runs `sql` with the streaming opcode and yields each row as a cell array in
column order, one server chunk in memory at a time. No parameters. See
[streaming.md](streaming.md) for the contract and the abandon rule.

### `Subscribe(string stream, string? after = null, int pollMs = 500)` → `IEnumerable<StreamEvent>`

Yields a stream's events forever, polling its log with a keyset cursor.
`StreamEvent` is a record `(string Id, string Op, object? Key, object? Ts,
object? Doc)`. `after` is a saved `Id` (an opaque string) to resume after.
See [streaming.md](streaming.md#following-a-stream).

### `Close()` / `Dispose()`

Terminal. Closes the socket; `IsOpen` and `IsUsable` become false; every
later call throws `ObjectDisposedException` (or `SkaidbException("connection
is not open")` from a command).

## `SkaidbCommand`

| Member | Meaning |
|---|---|
| `CommandText` | SQL with `?` placeholders. |
| `Parameters` | `List<object?>`, bound to the placeholders in order. |
| `Consistency` | This command's level; defaults to the connection's at creation. |
| `ExecuteReader()` | `SkaidbDataReader` over the rows. A non-row statement gives an empty reader (`FieldCount` 0, `Read()` false). |
| `ExecuteNonQuery()` | `int` rows affected for INSERT/UPDATE/DELETE; `0` for DDL and SELECT. |
| `ExecuteScalar()` | `object?`: first column of the first row; `null` for no rows or a NULL cell. |
| `ExecuteBatch(IReadOnlyList<IReadOnlyList<object?>> rows)` | `long` total affected, one round-trip. |
| `Dispose()` | Owns nothing; present for `using`. |

Behaviour of the three `Execute*` methods:

1. If the connection's transport died earlier, or the server closed the
   socket while it sat idle, the connection re-dials first (seed walk,
   handshake, Hello, `USE`), clearing the prepared-statement cache.
2. With parameters, the statement is prepared on the server (once per SQL
   text per connection, cached up to 240 entries) and executed with the
   values encoded as typed skaidb values. `statement expects N parameters,
   got M` is thrown before anything is sent when the counts differ.
3. If the server answers the prepare with `statement kind cannot be
   prepared` (DDL, `USE`, `SHOW`, …) the same `?` parameters are rendered as
   SQL literals client-side (`'` doubled, `byte[]` as hex, timestamps as epoch
   milliseconds; arrays and documents cannot be rendered) and the text is
   sent. Any *other* prepare error is the statement's error and is thrown.
4. Without parameters, the text is sent as-is. A `?` outside a string
   literal with no parameters is an error (`query has placeholders but no
   parameters given`).
5. The reply is decoded: `Rows`, `ResultSets`, `Mutation`, `Ddl`, or `Error`
   → `SkaidbException(message)`.

`ExecuteBatch`: prepares (a non-preparable statement throws
`UnpreparableException`, a `SkaidbException` subclass), checks every row's
arity (`batch row expects N parameters, got M`), sends one
`OP_EXECUTE_BATCH`, and returns the total. Rows autocommit individually; on
the first failing row the server's error names the row index and how many
applied before it. An empty list returns 0 without a round-trip.

Statements on a connection are serialized by a lock. While a `Stream()` is
open, a statement from another thread throws `connection is busy streaming`
instead of waiting.

## `SkaidbDataReader`

Forward-only, fully buffered (the whole result set arrived in one frame).

| Member | Meaning |
|---|---|
| `FieldCount` | Columns in the current result set. |
| `Read()` | Advance; `false` when exhausted. |
| `NextResult()` | Advance to the next result set of a multi-set reply (a `CALL` whose body `EMIT`s); `false` when there is none. |
| `GetName(i)`, `GetOrdinal(name)` | Column name / index. `GetOrdinal` tries an exact match first, then case-insensitive; throws when absent. |
| `IsDBNull(i)` | The cell is NULL. |
| `GetValue(i)`, `this[int]`, `this[string]` | The raw mapped value; NULL is `DBNull.Value`. |
| `GetBoolean`, `GetInt32`, `GetInt64`, `GetDouble`, `GetDecimal`, `GetString`, `GetGuid`, `GetDateTimeOffset`, `GetBytes` | Typed getters; `SkaidbException` on NULL. Numeric getters convert between numeric types (`GetInt32` of a `long`, `GetDecimal` of a decimal-string). |
| `Dispose()` | Owns nothing. |

Accessing a cell before the first `Read()` or after the last throws
`SkaidbException`.

## `SkaidbConnectionPool`

```csharp
new SkaidbConnectionPool(string connectionString, int maxsize = 10);
SkaidbConnection Acquire();
void Release(SkaidbConnection c);
T WithConnection<T>(Func<SkaidbConnection, T> work);
void Dispose();
```

See [pooling.md](pooling.md).

## `SkaidbConsistency`

`enum SkaidbConsistency : byte { One = 0, Quorum = 1, All = 2 }` — the wire
values.

## `SkaidbException`, `UnpreparableException`

`SkaidbException : Exception` is thrown for every driver and server error;
`InnerException` carries the socket/IO exception for transport failures.
`UnpreparableException : SkaidbException` is what `ExecuteBatch` throws for a
statement kind the server will not prepare (the `Execute*` methods handle it
internally by falling back to text).

## Thread safety

A `SkaidbConnection` may be used from several threads, but statements are
serialized and a stream holds the connection; concurrency comes from a pool.
`SkaidbConnectionPool` is thread-safe. `SkaidbCommand` and
`SkaidbDataReader` are not.
