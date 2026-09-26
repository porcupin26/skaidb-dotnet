# API reference

```csharp
using Skaidb;
```

Everything lives in the `Skaidb` namespace: `SkaidbConnection`,
`SkaidbConnectionStringBuilder`, `SkaidbCommand`, `SkaidbParameter`,
`SkaidbParameterCollection`, `SkaidbDataReader`, `SkaidbBatch`,
`SkaidbBatchCommand`, `SkaidbBatchCommandCollection`, `SkaidbFactory`,
`SkaidbConnectionPool`, `SkaidbConsistency`, `SkaidbAuthMechanism`,
`SkaidbException`, `UnpreparableException`. The ADO.NET types derive from
`System.Data.Common`; [adonet.md](adonet.md) covers how they behave as a
provider.

## `SkaidbConnection`

### Constructors

```csharp
new SkaidbConnection(string connectionString);
new SkaidbConnection();                     // closed, defaults; set ConnectionString
new SkaidbConnection(string host = "localhost", int port = 7000, string user = "anonymous",
                     string password = "", SkaidbConsistency consistency = SkaidbConsistency.Quorum,
                     string database = "", bool tls = false, string tlsCa = "",
                     bool tlsInsecure = false, string tlsServerName = "skaidb");
```

Connection string keys (`;`-separated `Key=Value`, case-insensitive, values
may be quoted; parsed by `SkaidbConnectionStringBuilder`; an unknown key or
an invalid value throws `SkaidbException`):

| Key | Aliases | Default | Meaning |
|---|---|---|---|
| `Host` | `Server`, `DataSource`, `Data Source` | `localhost` | One host or a comma list (`a,b:7001,[::1]:7002`); every entry is a seed, one without a port gets `Port`. |
| `Port` | | `7000` | Port for hosts that carry none. A `Seeds` entry without a port also gets `7000`. |
| `Seeds` | | — | Comma-separated `host:port` list; overrides `Host`/`Port`. Shuffled on every connect. |
| `Username` | `User`, `UserId`, `User Id`, `Uid` | `anonymous` | |
| `Password` | `Pwd` | `""` | Empty skips mutual authentication (anonymous). |
| `Database` | `Db` | `""` | `USE "<database>"` after every (re)connect. |
| `Consistency` | | `Quorum` | `One`/`Quorum`/`All` or `0`/`1`/`2`, case-insensitive. |
| `Timeout` | `ConnectTimeout`, `Connect Timeout` | `10` | Seconds (decimals allowed). Connect, and every socket read/write. |
| `Tls` | | `false` | `true`/`1`: TLS with the system trust store. |
| `TlsCa` | `Tls Ca` | `""` | PEM bundle path; implies `Tls`. |
| `TlsInsecure` | `Tls Insecure` | `false` | No certificate verification; implies `Tls`. |
| `TlsServerName` | `Tls Server Name` | `skaidb` | SNI and verification name. |
| `TlsClientCert` | `Tls Client Cert`, `TlsClientCertificate` | `""` | PEM client certificate to present; implies `Tls`. |
| `TlsClientKey` | `Tls Client Key` | `""` | Its PEM private key; empty when the key is in the certificate file. |
| `AuthMechanism` | `Auth Mechanism` | `Scram` | `Scram` (`password`), or `Certificate` (`external`, `x509`): the client certificate is the login. |

The constructor only stores configuration; nothing is dialled until `Open()`.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `Host`, `Port`, `User` | | As configured (`Host` is the first host of the list, without its port). |
| `Seeds` | `IReadOnlyList<string>` | The endpoints tried, `host:port`. |
| `Database`, `Tls`, `TlsCa`, `TlsInsecure`, `TlsServerName` | | As configured. |
| `Consistency` | `SkaidbConsistency` | Read/write. Default for commands created afterwards. |
| `Timeout` | `TimeSpan` | Read/write; set before `Open()`. |
| `IsOpen` | `bool` | The handshake completed and neither `Close()` nor `Dispose()` was called since. |
| `IsUsable` | `bool` | `IsOpen`, and: no transport error pending, no `Stream()` in flight, and the socket not closed by the peer. What a pool checks. |
| `StreamColumns` | `IReadOnlyList<string>` | Column names of the most recent `Stream()` call, set once its header arrives; empty when it returned no rows. |
| `StreamAffected` | `long?` | Affected-row count of the most recent `Stream()` call when it was a mutation; `null` otherwise. |
| `AuthMechanism` | `SkaidbAuthMechanism` | `Scram` or `Certificate`. |
| `ConnectionString` | `string` | The normalised connection string (canonical key names). Settable while closed. |
| `State` | `ConnectionState` | `Closed`, `Open`, or `Open \| Fetching` while a `Stream()` is in flight. |
| `DataSource` | `string` | The seeds, comma-separated. |
| `ServerVersion` | `string` | Empty: the wire protocol does not report it (`SELECT version()` does). |
| `DriverName` | `const string` | `"dotnet"`. |
| `DriverVersion` | `static string` | The package version, from the assembly's informational version. |

### `Open()`

Shuffles the seeds and tries each (TCP connect, then TLS if configured)
until one connects; authenticates (SCRAM-SHA-256, or with
`AuthMechanism=Certificate` the EXTERNAL mechanism: the TLS client
certificate is the credential, the username sent is `Username` when one was
given and empty otherwise, and the outcome's placeholder signature is not
checked); sends the Hello frame
(driver name `dotnet`, version `DriverVersion`; ignored by servers that
predate it); runs `USE "<database>"` if `Database` was given. Idempotent:
a second call on an open connection returns at once. Throws
`SkaidbException` — `no reachable endpoint in <seeds>: <last error>`,
`authentication denied: …`, `server signature mismatch (mutual auth failed)`,
`connect failed: …`, `certificate authentication needs TLS with a client
certificate …` — and `ObjectDisposedException` after `Dispose()`. Raises
`StateChange` (Closed → Open).

### `CreateCommand()` → `SkaidbCommand`

A command bound to this connection, with `Consistency` copied from it.
(`DbConnection.CreateCommand()` returns the same object typed as `DbCommand`.)

### `CreateBatch()` → `SkaidbBatch`

A batch bound to this connection; see [adonet.md](adonet.md#dbbatch).

### `ChangeDatabase(string name)`

Runs `USE "<name>"` now when open, and on every later dial.

### `BeginTransaction()`

Throws `NotSupportedException`: the wire protocol has no transactions.

### `Stream(string sql, SkaidbConsistency? consistency = null)` → `IEnumerable<object?[]>`

Runs `sql` with the streaming opcode and yields each row as a cell array in
column order, one server chunk in memory at a time. No parameters. See
[streaming.md](streaming.md) for the contract and the abandon rule.

### `Subscribe(string stream, string? after = null, int pollMs = 500)` → `IEnumerable<StreamEvent>`

Yields a stream's events forever, polling its log with a keyset cursor.
`StreamEvent` is a record `(string Id, string Op, object? Key, object? Ts,
object? Doc)`. `after` is a saved `Id` (an opaque string) to resume after.
See [streaming.md](streaming.md#following-a-stream).

### `Close()`

Closes the socket and drops the prepared-statement cache; `IsOpen` and
`IsUsable` become false and `State` is `Closed`. The settings stay: `Open()`
dials again. Raises `StateChange` (Open → Closed). A command on a closed
connection throws `SkaidbException("connection is not open")`; a command
with no connection at all throws `InvalidOperationException`.

### `Dispose()`

`Close()`, and final: every later `Open()` throws `ObjectDisposedException`.

## `SkaidbCommand`

| Member | Meaning |
|---|---|
| `new SkaidbCommand(conn)`, `new SkaidbCommand()`, `new SkaidbCommand(text, conn?)` | Bound, unbound (set `Connection`), or with its text. |
| `CommandText` | SQL with `?`, `@name` or `$N` placeholders (one style per statement). |
| `CommandType` | `Text`, or `StoredProcedure`: `CommandText` is a procedure name, run as `CALL name(?, …)` with one placeholder per parameter. Others throw `NotSupportedException`. |
| `Parameters` | `SkaidbParameterCollection`. `Add(value)` adds a positional value (or a `SkaidbParameter` as itself), `AddWithValue(name, value)` a named one. |
| `Connection` | The `SkaidbConnection` it runs on. |
| `Consistency` | This command's level; defaults to the connection's at creation. |
| `CommandTimeout` | Stored for ADO.NET callers; the connection's `Timeout` bounds each request. |
| `Transaction` | Always `null`; setting a non-null value throws `NotSupportedException`. |
| `Prepare()` | Prepares on the server now (otherwise it happens on first execution). |
| `Cancel()` | No-op. |
| `ExecuteReader()`, `ExecuteReader(CommandBehavior)` | `SkaidbDataReader` over the rows. A non-row statement gives an empty reader (`FieldCount` 0, `Read()` false, `RecordsAffected` the count for a mutation and -1 for DDL). `CommandBehavior.CloseConnection` closes the connection with the reader. |
| `ExecuteNonQuery()` | `int` rows affected for INSERT/UPDATE/DELETE; `0` for DDL and SELECT. |
| `ExecuteScalar()` | `object?`: first column of the first row; `null` for no rows or a NULL cell. |
| `ExecuteBatch(IReadOnlyList<IReadOnlyList<object?>> rows)` | `long` total affected, one round-trip. |
| `Dispose()` | Owns nothing; present for `using`. |

Behaviour of the three `Execute*` methods:

0. `@name` and `$N` placeholders are rewritten to `?` (outside `'…'`
   literals, `"…"` identifiers and `--`/`/* */` comments) with the values
   in placeholder order: `@name` matches `ParameterName` with or without its
   `@` (exactly first, then case-insensitively) and may repeat; `$N` is the
   Nth parameter. Errors: `no parameter named '@x'`, `placeholder $N has no
   parameter (M given)`, `a statement cannot mix ? placeholders with @name
   or $N placeholders`. A `?`-only statement passes unchanged.
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
| `RecordsAffected` | Rows changed by a mutation; -1 for a SELECT or DDL. |
| `HasRows`, `IsClosed`, `Depth` (0) | |
| `Read()` | Advance; `false` when exhausted. |
| `NextResult()` | Advance to the next result set of a multi-set reply (a `CALL` whose body `EMIT`s); `false` when there is none. |
| `GetName(i)`, `GetOrdinal(name)` | Column name / index. `GetOrdinal` tries an exact match first, then case-insensitive; throws when absent. |
| `IsDBNull(i)` | The cell is NULL. |
| `GetValue(i)`, `this[int]`, `this[string]` | The raw mapped value; NULL is `DBNull.Value`. |
| `GetBoolean`, `GetByte`, `GetInt16`, `GetInt32`, `GetInt64`, `GetFloat`, `GetDouble`, `GetDecimal`, `GetString`, `GetChar`, `GetGuid`, `GetDateTime`, `GetDateTimeOffset`, `GetBytes(i)` | Typed getters; `SkaidbException` on NULL. Numeric getters convert between numeric types (`GetInt32` of a `long`, `GetDecimal` of a decimal-string); `GetDateTime` is the timestamp in UTC. |
| `GetFieldValue<T>(i)` | The value as `T`: exact type, numeric conversion, timestamp → `DateTime` (UTC), text → `Guid`, decimal-string → `decimal`. NULL gives `null` for a reference or nullable `T` and throws `SkaidbException` otherwise. |
| `GetBytes(i, offset, buffer, bufferOffset, length)`, `GetChars(…)` | The ADO.NET chunked forms (a null buffer returns the total length). |
| `GetValues(object[])` | Copies the row (NULL as `DBNull.Value`). |
| `GetFieldType(i)`, `GetDataTypeName(i)` | The wire format carries no column types: the type of the column's first non-NULL value in the current set (`object`/`unknown` when all are NULL), and its skaidb name (`int`, `float`, `decimal`, `string`, `bool`, `bytes`, `uuid`, `timestamp`, `array`, `document`). |
| `GetSchemaTable()` | A minimal `DataTable`: `ColumnName`, `ColumnOrdinal`, `ColumnSize` (-1), `DataType`, `DataTypeName`, `AllowDBNull` (true). |
| `Close()`, `Dispose()` | Mark the reader closed (then `Read()` throws `InvalidOperationException`); close the connection under `CommandBehavior.CloseConnection`. |

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

`SkaidbException : DbException` is thrown for every driver and server error;
`InnerException` carries the socket/IO exception for transport failures.
`UnpreparableException : SkaidbException` is what `ExecuteBatch` throws for a
statement kind the server will not prepare (the `Execute*` methods handle it
internally by falling back to text).

## `SkaidbAuthMechanism`

`enum SkaidbAuthMechanism : byte { Scram = 0, Certificate = 2 }` — the wire
mechanism byte of `AuthStart`.

## `SkaidbConnectionStringBuilder`, `SkaidbParameter`, `SkaidbBatch`, `SkaidbFactory`

See [adonet.md](adonet.md).

## Thread safety

A `SkaidbConnection` may be used from several threads, but statements are
serialized and a stream holds the connection; concurrency comes from a pool.
`SkaidbConnectionPool` is thread-safe. `SkaidbCommand` and
`SkaidbDataReader` are not.
