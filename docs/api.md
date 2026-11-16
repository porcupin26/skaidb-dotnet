# API reference

```csharp
using Skaidb;
```

Everything lives in the `Skaidb` namespace: `SkaidbConnection`,
`SkaidbConnectionStringBuilder`, `SkaidbCommand`, `SkaidbParameter`,
`SkaidbParameterCollection`, `SkaidbDataReader`, `SkaidbBatch`,
`SkaidbBatchCommand`, `SkaidbBatchCommandCollection`, `SkaidbFactory`,
`SkaidbConnectionPool`, `SkaidbPoolOptions`, `SkaidbPoolMember`,
`SkaidbLoadBalancing`, `SkaidbIdleOrder`, `SkaidbMemberState`,
`SkaidbConsistency`, `SkaidbAuthMechanism`, `SkaidbException`,
`SkaidbTransactionConflictException`, `UnpreparableException`. The ADO.NET
types derive from
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
| `Database` | `Db` | `""` | `USE "<database>"` after every (re)connect; the property follows `ChangeDatabase()` and `USE` statements (see [`ChangeDatabase`](#changedatabasestring-name)). |
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
| `IsUsable` | `bool` | `IsOpen`, and: no transport error pending, no `Stream()` in flight, and nothing to read on the idle socket (the peer closed or reset it, or sent bytes nobody asked for). What a pool checks. |
| `StreamColumns` | `IReadOnlyList<string>` | Column names of the most recent `Stream()` call, set once its header arrives; empty when it returned no rows. |
| `StreamAffected` | `long?` | Affected-row count of the most recent `Stream()` call when it was a mutation; `null` otherwise. |
| `StreamResultSets` | `SkaidbDataReader?` | Every result set of the most recent `Stream()` call when the server answered it with several in one frame (a `CALL` whose body `EMIT`s), as the reader `ExecuteReader()` returns: on the first set, `NextResult()` walking the rest; `null` otherwise. |
| `AuthMechanism` | `SkaidbAuthMechanism` | `Scram` or `Certificate`. |
| `ConnectionString` | `string` | The normalised connection string (canonical key names). Settable while closed. |
| `State` | `ConnectionState` | `Closed`, `Open`, or `Open \| Fetching` while a `Stream()` is in flight. |
| `DataSource` | `string` | The seeds, comma-separated. |
| `ServerVersion` | `string` | Empty: the wire protocol does not report it (`SELECT version()` does). |
| `DriverName` | `const string` | `"dotnet"`. |
| `DriverVersion` | `static string` | The package version, from the assembly's informational version. |

### `Open()`

Shuffles the seeds and tries each (TCP connect, TLS if configured, the
handshake) until one connects and authenticates — a node that accepts the
connection but drops the handshake (at its connection ceiling, or going
down) is skipped like an unreachable one, while a refused login stops the
walk. It authenticates (SCRAM-SHA-256, or with
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
`StateChange` (Closed → Open). A call that throws leaves the connection
closed — also when the handshake succeeded and it was the `USE` that the
server refused — so `Open()` can simply be called again.

### `CreateCommand()` → `SkaidbCommand`

A command bound to this connection, with `Consistency` copied from it.
(`DbConnection.CreateCommand()` returns the same object typed as `DbCommand`.)

### `CreateBatch()` → `SkaidbBatch`

A batch bound to this connection; see [adonet.md](adonet.md#dbbatch).

### `ChangeDatabase(string name)`

Runs `USE "<name>"` now when open, and on every later dial.

A `USE` sent as a statement does the same. When the server has accepted a
statement that is exactly `USE [DATABASE] <name>` — the name bare or in
double quotes (`""` for a quote inside), an optional `;` behind it, `--`
comments skipped as the server skips them (`USE app -- the app database`)
— `Database` becomes that name, and every later dial enters it before
anything else is sent: a re-dial does not drop the session back into the
default database. A text the driver cannot read as one such statement (a
second statement behind it, a qualified name) changes nothing it knows;
use `ChangeDatabase()` there. If the database cannot be
entered on a new connection (it was dropped meanwhile), the statement that
needed the dial throws that error and is not sent, and so does every
statement after it until the database exists again — or until the session
is moved. A `USE` statement the driver can read, and `ChangeDatabase()`,
are the exception to "the database first": on a new connection they are
sent without entering the database they leave (the session's `SET`
settings still go first), and their answer decides the session database as
usual. A `USE` that is refused there leaves `Database` as it was, and the
next statement that is not a `USE` enters it first, or fails.

### Session settings: `SET CONSISTENCY`, `SET SCAN BUDGET`

Two more things live on the session and are noted the same way, from the
statement that sets them. When the server has accepted a text that is
exactly one statement whose first words are `SET CONSISTENCY`, or `SET SCAN
BUDGET` — keywords in any case, free whitespace, an optional `;` behind it,
`--` comments skipped — the driver keeps that statement, without its
comments, as the session's consistency override or scan budget: one of each
kind, the last one accepted. A statement whose next word is `DEFAULT`
clears what was kept. A statement run with a parameter (`SET SCAN BUDGET
ROWS @n`) is kept as the text that was sent, the value in it. Any other
text changes nothing the driver knows.

On every re-dial the driver sends, after the `USE` and before the statement
that needed the dial, the kept `SET CONSISTENCY …` and then the kept `SET
SCAN BUDGET …`. If the new connection refuses one, the statement throws
that error and is not sent — it does not run under other settings than the
application made — and the next statement dials again.

`Close()` ends the session and forgets both; `Database` is a property of
the connection and stays.

### `Transaction<T>(Func<SkaidbConnection, T> body, int attempts = 3, string begin = "BEGIN")` → `T`

Runs `body` inside a transaction on this connection and returns its value;
`Transaction(Action<SkaidbConnection> body, int attempts = 3, string begin =
"BEGIN")` is the same for a body that returns nothing. In order:

1. `attempts` below 1 throws `ArgumentOutOfRangeException`, a null `body` or
   `begin` `ArgumentNullException`, and a body whose value is a `Task` or a
   `ValueTask` (what an `async` lambda returns) `ArgumentException` — all
   before anything is sent.
2. `begin` is sent, as given. If it fails its error is thrown: the body does
   not run and nothing else is sent (its statements would each autocommit).
3. `body(this)` runs. It runs its statements on the connection it is handed.
4. If the body throws, `ROLLBACK` is sent, whatever it answers is ignored,
   and the body's exception is rethrown unchanged. There is no retry, also
   when that exception is a `SkaidbTransactionConflictException`.
5. Otherwise `COMMIT` is sent. On success the body's value is returned. On
   a `SkaidbTransactionConflictException` with attempts left, the helper
   goes back to step 2 at once, with no wait: the body runs again from the
   start, so it must be safe to run more than once and should do nothing
   outside the database that a second run would repeat; the value returned
   is the last run's. On the last attempt the conflict is thrown. Any other
   `COMMIT` error is thrown as it is, with no retry and no `ROLLBACK`: a
   `COMMIT` that fails has ended the transaction.

Plain `BEGIN` opens a transaction on a standalone server; on a cluster pass
`BEGIN ATOMIC` or `BEGIN ATOMIC PARTITION (col = lit)` as `begin`.

A transport failure is the usual connection error and is never retried.
Between `begin` and the end of the transaction the connection does not
re-dial: a statement that finds the socket closed (see step 1 under
[`SkaidbCommand`](#skaidbcommand)) throws `connection lost inside a
transaction: …` instead, because a new connection is in autocommit and would
apply the remaining statements one by one. `Open()` throws the same when the
body closed the connection itself: nothing is dialed inside a transaction.
The next statement after the helper has returned re-dials as usual (after a
`Close()` in the body, `Open()` comes first, as after any `Close()`).

The body is synchronous. A `Stream()` started in it is enumerated in it, not
returned from it. One the body leaves open — started, and neither run out
nor disposed — holds the connection, so neither `COMMIT` nor `ROLLBACK` can
be sent: the helper throws (the body's exception, else `connection is busy
streaming: …`) and closes the socket, which is what ends the transaction on
the server; that stream is over with its socket, and the connection re-dials
on its next statement. Other threads must leave the connection alone meanwhile:
a statement they ran on it would be part of the transaction.

### A transaction written by hand

`BEGIN`, the statements and `COMMIT` or `ROLLBACK` can also be sent as
ordinary statements, and the driver notes such a transaction from what goes
by, as it notes a `USE`:

- a statement the server accepted whose first word is `BEGIN` (`BEGIN`,
  `BEGIN TRANSACTION`, `BEGIN ATOMIC …`) opens it;
- a statement that is exactly `COMMIT [TRANSACTION]` or `ROLLBACK
  [TRANSACTION]` ends it once the server has answered it, with success or
  with an error (a refused `COMMIT` has ended the transaction).

Keywords in any case, an optional `;`, `--` comments skipped; run through
a command, a batch or `Stream()` alike. While the transaction is open the
rule of `Transaction()` holds: the connection does not re-dial. Lost under
a statement, that statement throws the transport error; found lost before
one (the look at the idle socket), or on any statement after, it throws
`connection lost inside a transaction: …` and nothing is sent. The
application ends it:

| Statement | Answer |
|---|---|
| `ROLLBACK` | Success, not sent: the server discarded the transaction with the connection. Also when the connection is lost under the `ROLLBACK` itself. |
| `COMMIT` | `connection lost inside a transaction: nothing was committed …`, not sent. |
| `COMMIT`, the connection lost under it | `connection lost under COMMIT: the outcome is unknown …` (the transport error is the inner exception): the server may have committed before its reply was lost. |

Either one ends the transaction, and the next statement re-dials as usual
(database and `SET` settings restored). `Close()` ends it with the session.
`Transaction()` opens and ends its transaction with the same statements, so
the two do not differ here; the helper additionally refuses `Open()` in its
body and is over when it returns.

### `BeginTransaction()`

Throws `NotSupportedException`: there is no `DbTransaction` object. A
transaction is the SQL statements `BEGIN` … `COMMIT` sent on one connection,
which `Transaction()` above runs.

### `Stream(string sql, SkaidbConsistency? consistency = null)` → `IEnumerable<object?[]>`

Runs `sql` with the streaming opcode and yields each row as a cell array in
column order, one server chunk in memory at a time. See
[streaming.md](streaming.md) for the contract and the abandon rule.

### `Stream(string sql, IReadOnlyList<object?>? parameters, SkaidbConsistency? consistency = null)` → `IEnumerable<object?[]>`

The same, with `parameters` bound to the statement's `?` placeholders in
order, typed (the values `SkaidbCommand.Parameters` takes), over
`OP_QUERY_STREAM_PARAMS`, which needs a server ≥ 0.330; an older one
answers `server does not support parameterised streaming (skaidb >=
0.330): …` and the connection stays usable. `null` or an empty list sends
the parameterless request. Pass a collection expression
(`conn.Stream(sql, [a, b])`), an `object?[]` or a list; a consistency stays
the trailing argument and is never taken for a parameter
(`Stream(sql, null)` is the overload above). See
[streaming.md](streaming.md#parameters).

### `Subscribe(string stream, string? after = null, int pollMs = 500)` → `IEnumerable<StreamEvent>`

Yields a stream's events forever, polling its log with a keyset cursor.
`StreamEvent` is a record `(string Id, string Op, object? Key, object? Ts,
object? Doc)`. `after` is a saved `Id` (an opaque string) to resume after.
See [streaming.md](streaming.md#following-a-stream).

### `Close()`

Closes the socket and drops the prepared-statement cache; `IsOpen` and
`IsUsable` become false and `State` is `Closed`. The connection's settings
stay, `Database` included: `Open()` dials again. The session is over, and
with it what `SET CONSISTENCY` and `SET SCAN BUDGET` statements set in it.
A `Stream()` still open ends with the socket: its enumerator
throws `the stream ended with its connection: …` when advanced, and
disposing it touches nothing of the connection `Open()` dials next. Raises
`StateChange` (Open → Closed). A command on a closed
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
| `Transaction` | Always `null`; setting a non-null value throws `NotSupportedException` (there is no `DbTransaction` object; see `SkaidbConnection.Transaction()`). |
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
1. If the connection's transport died earlier, or the look at the idle
   socket finds it dead (a zero-timeout poll before anything is written:
   closed or reset by the peer, or holding bytes nobody asked for), the
   connection re-dials first (seed walk, handshake, Hello, `USE`, the
   session's `SET CONSISTENCY` and `SET SCAN BUDGET`), clearing the
   prepared-statement cache. Inside a transaction —
   `SkaidbConnection.Transaction()`'s, or one a `BEGIN` statement opened —
   it throws instead, and a `ROLLBACK` is answered without being sent (see
   "A transaction written by hand").
2. With parameters, the statement is prepared on the server (once per SQL
   text per connection; the connection keeps up to 240 prepared, and closes
   the one used longest ago on the server to make room for another) and
   executed with the values encoded as typed skaidb values. `statement expects N parameters,
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
   → `SkaidbException(message)`, or its subclass
   `SkaidbTransactionConflictException(message)` when the message starts
   with `transaction conflict:`.

`ExecuteBatch`: prepares (a non-preparable statement throws
`UnpreparableException`, a `SkaidbException` subclass), checks every row's
arity (`batch row expects N parameters, got M`), sends one
`OP_EXECUTE_BATCH`, and returns the total. Rows autocommit individually; on
the first failing row the server's error names the row index and how many
applied before it. With a transaction open on the connection the rows join
it: they commit or roll back with it, and a failing row leaves the
transaction open with the earlier rows in it. An empty list returns 0
without a round-trip.

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
new SkaidbConnectionPool(string connectionString, SkaidbPoolOptions options);
SkaidbConnection Acquire();
SkaidbConnection Acquire(object? affinity);
void Release(SkaidbConnection c);
T WithConnection<T>(Func<SkaidbConnection, T> work);
T WithConnection<T>(object? affinity, Func<SkaidbConnection, T> work);
T Transaction<T>(Func<SkaidbConnection, T> body, int attempts = 3, string begin = "BEGIN");
void Transaction(Action<SkaidbConnection> body, int attempts = 3, string begin = "BEGIN");
IReadOnlyList<SkaidbPoolMember> Members();
void Dispose();
```

`Transaction()` is `SkaidbConnection.Transaction()` on one checked-out
connection: the argument errors are thrown before the checkout, the
connection is held for the whole transaction, every attempt included, and
released afterwards. Under an affinity key, call
`WithConnection(affinity, c => c.Transaction(body))`.

Thread-safe. The connection string's `Host`/`Seeds` are the starting members
and every connection is built from it; an invalid connection string or
option throws `SkaidbException` from the constructor. `maxsize` caps the
**idle** connections kept; it does not bound concurrent checkouts, and
`Acquire()` never blocks. `affinity` scopes `ReadYourWrites` (any object with
value equality; null = the pool-wide key). `Release()` disposes a connection
that is broken, belongs to an ejected/not-ready/departed member, was not
handed out by this pool, or would exceed `maxsize`. Before it keeps one, it
puts the session back into the pool's own — `ROLLBACK` of a transaction
left open, `USE` of the pool's database (`default` when the connection
string names none), `SET SCAN BUDGET DEFAULT`, `SET CONSISTENCY DEFAULT`,
each only if the borrower changed it — and disposes the connection if that
fails ([pooling.md](pooling.md#semantics)). `Dispose()` closes the
idle connections and stops the background timer that re-probes ejected
members and refreshes discovery.

```csharp
public sealed class SkaidbPoolOptions          // every property is init-only
{
    int MaxSize = 10;
    SkaidbLoadBalancing LoadBalancing = Random;  // Random | RoundRobin | LeastLatency
    SkaidbIdleOrder IdleOrder = Lifo;            // Lifo | Fifo
    bool Discover = false;                       // members from SHOW ENDPOINTS
    TimeSpan RefreshInterval = 60 s;             // between discovery refreshes (5 s while a member is not ready)
    TimeSpan ReadYourWrites = Zero;              // affinity window after a write; Zero = off
    int EjectErrors = 3;                         // consecutive transport failures
    double EjectLatencyFactor = 3.0;             // x the other members' median; 0 = off
    TimeSpan EjectLatencyFloor = 50 ms;          // above the median, at least
    TimeSpan EjectBackoff = 5 s;                 // first ejection, doubling per repeat
    TimeSpan EjectBackoffMax = 60 s;
}

public sealed record SkaidbPoolMember(
    string Endpoint,            // what the pool dials, host:port
    string? Canonical,          // the member's own SHOW ENDPOINTS name, once discovered
    SkaidbMemberState State,    // Ready | Ejected | Resyncing | Unreachable | Unlisted
    TimeSpan? Latency,          // round-trip average; null until measured
    int InUse,                  // checked out now
    int Idle,                   // idle in the pool
    bool Seed);                 // from the connection string, not discovered
```

See [pooling.md](pooling.md) for what each policy and option does.

## `SkaidbConsistency`

`enum SkaidbConsistency : byte { One = 0, Quorum = 1, All = 2 }` — the wire
values.

## `SkaidbException`, `SkaidbTransactionConflictException`, `UnpreparableException`

`SkaidbException : DbException` is thrown for every driver and server error;
`InnerException` carries the socket/IO exception for transport failures.
`SkaidbTransactionConflictException : SkaidbException` is a server error
whose message starts with `transaction conflict:` — the server refusing a
`COMMIT` because another writer changed a row the transaction writes after
its `BEGIN` — from whichever call received it: a command, a batch or a
stream. `Message` is the server's message, unchanged; only the prefix is
matched. Nothing was applied, the transaction is over and the connection is
back in autocommit and usable (to a pool it is a round trip that succeeded,
not a failure of the member), so running the whole transaction again is
safe; `Transaction()` does. It is the one exception whose
`DbException.IsTransient` is true: a provider-agnostic retry layer may run
the unit of work again. The unit is the transaction — the refused `COMMIT`
sent again alone is answered `no transaction in progress`, an error, never
a success. Every other `SkaidbException` reports false: a statement error
is not cured by a retry, and a statement lost with its connection may have
run.
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
