# ADO.NET

The driver is an ADO.NET provider: its types derive from
`System.Data.Common`, so code written against `DbConnection`, `DbCommand`
and `DbDataReader` — or a library built on them, such as Dapper — runs on
skaidb unchanged.

| skaidb type | ADO.NET base |
|---|---|
| `SkaidbConnection` | `DbConnection` |
| `SkaidbConnectionStringBuilder` | `DbConnectionStringBuilder` |
| `SkaidbCommand` | `DbCommand` |
| `SkaidbParameter`, `SkaidbParameterCollection` | `DbParameter`, `DbParameterCollection` |
| `SkaidbDataReader` | `DbDataReader` |
| `SkaidbBatch`, `SkaidbBatchCommand`, `SkaidbBatchCommandCollection` | `DbBatch`, `DbBatchCommand`, `DbBatchCommandCollection` |
| `SkaidbFactory` | `DbProviderFactory` |
| `SkaidbException` | `DbException` |

## Registering the provider

```csharp
using System.Data.Common;
using Skaidb;

// Once, at startup. The invariant name is "Skaidb".
DbProviderFactories.RegisterFactory(SkaidbFactory.ProviderInvariantName, SkaidbFactory.Instance);
// or: SkaidbFactory.Register();
// or by type: DbProviderFactories.RegisterFactory("Skaidb", typeof(SkaidbFactory));

DbProviderFactory factory = DbProviderFactories.GetFactory("Skaidb");
using DbConnection conn = factory.CreateConnection()!;
conn.ConnectionString = "Host=db1,db2,db3;Username=app;Password=secret;Database=app";
conn.Open();

using DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "SELECT name FROM users WHERE id = @id";
DbParameter p = cmd.CreateParameter();
p.ParameterName = "@id";
p.Value = 1L;
cmd.Parameters.Add(p);
string? name = (string?)cmd.ExecuteScalar();
```

`SkaidbFactory` creates connections, commands, parameters, connection
string builders, batches and batch commands; it has no data adapter or
command builder. `DbProviderFactories.GetFactory(conn)` returns
`SkaidbFactory.Instance` for any `SkaidbConnection`.

## Connection strings

`SkaidbConnectionStringBuilder` parses and builds connection strings with
typed properties (`Host`, `Port`, `Username`, `Password`, `Database`,
`Consistency`, `Timeout`, `Seeds`, `Tls`, `TlsCa`, `TlsInsecure`,
`TlsServerName`, `TlsClientCert`, `TlsClientKey`, `AuthMechanism`). Keys are
case-insensitive, aliases (`Server`, `User Id`, `Pwd`, …) resolve to the
canonical name, and the builder writes canonical names back out. An unknown
key, or a value that does not parse (`Port=x`, `Consistency=maybe`,
`Timeout=-1`, `AuthMechanism=kerberos`), throws `ArgumentException`; a
`SkaidbConnection` given such a string throws `SkaidbException` instead.
The full key table is in [api.md](api.md#constructors).

`Host` takes a comma list: `Host=a,b:7001,[::1]:7002;Port=7000` dials `a:7000`,
`b:7001` and `[::1]:7002` as seeds (shuffled on every connect).

## Connection lifecycle

- `Open()` dials and authenticates; it is idempotent.
- `Close()` releases the socket and keeps the settings; `Open()` dials
  again. This is what lets Dapper and other callers that open and close a
  connection around each call work.
- `Dispose()` is final.
- `State` is `Closed`, `Open`, or `Open | Fetching` while a
  `Stream()` holds the connection; `StateChange` fires on `Open()` and
  `Close()`.
- `ConnectionString` can be changed only while closed.
- `ChangeDatabase(name)` runs `USE` and makes it the database for every
  later dial.
- `ServerVersion` is empty: the protocol does not report it.

## Commands and parameters

Placeholders are `?` (positional), `@name` (by `ParameterName`, with or
without its `@`) or `$1`, `$2`, … (by position). The server's prepared
statements know only `?`, so the driver rewrites the other two before
preparing, leaving `'…'` literals, `"…"` identifiers and comments alone.

```csharp
cmd.CommandText = "UPDATE users SET name = @name WHERE id = @id OR parent = @id";
cmd.Parameters.AddWithValue("@name", "Ada");
cmd.Parameters.AddWithValue("@id", 1L);        // used twice

cmd.CommandText = "SELECT $2, $1";
cmd.Parameters.Add("first");                   // a bare value is a positional parameter
cmd.Parameters.Add("second");
```

- `SkaidbParameter` is input-only (`Direction` other than `Input` throws
  `NotSupportedException`). `DbType` is informational: the value's own .NET
  type decides its wire type ([types.md](types.md)); when unset it is
  inferred from the value.
- `CommandType.StoredProcedure` runs `CALL <CommandText>(?, …)` with one
  placeholder per parameter, in order.
- `CommandTimeout` is stored but the connection's `Timeout` bounds each
  request; `Cancel()` is a no-op.
- `Prepare()` prepares the statement on the server now; otherwise that
  happens on first execution and is cached per connection.

## Reading results

`SkaidbDataReader` is fully buffered: the result has arrived before
`ExecuteReader()` returns, so reading never touches the socket.

- `GetFieldValue<T>` converts: exact type, any numeric conversion, a
  timestamp to `DateTime` (UTC) or `DateTimeOffset`, text to `Guid`, a
  wide-decimal string to `decimal`. NULL gives `null` for a reference or
  nullable `T`.
- `GetFieldType(i)` is the type of the column's first non-NULL value (the
  protocol has no column types); `GetDataTypeName(i)` its skaidb name.
- `GetSchemaTable()` returns one row per column with `ColumnName`,
  `ColumnOrdinal`, `ColumnSize` (-1), `DataType`, `DataTypeName` and
  `AllowDBNull`.
- `NextResult()` walks the result sets of a `CALL` whose body `EMIT`s, and
  of a batch.
- `RecordsAffected` is the mutation count, -1 for a SELECT or DDL.
- `ExecuteReader(CommandBehavior.CloseConnection)` closes the connection
  when the reader closes. Other behaviours are accepted and change nothing.

## DbBatch

```csharp
using var batch = conn.CreateBatch();
foreach (var u in users)
{
    var c = batch.BatchCommands.Add("INSERT INTO users (id, name) VALUES (@id, @name)");
    c.Parameters.AddWithValue("id", u.Id);
    c.Parameters.AddWithValue("name", u.Name);
}
batch.BatchCommands.Add("SELECT count(*) FROM users");
int inserted = batch.ExecuteNonQuery();
```

Consecutive commands with the same statement text (after placeholder
rewriting) and parameters go to the server as ONE `OP_EXECUTE_BATCH`
round-trip; every other command runs as its own statement, in order.
`ExecuteNonQuery()` returns the total rows changed; `ExecuteReader()` gives
one result set per row-producing command; `ExecuteScalar()` the first value
of the first. A command merged into an `OP_EXECUTE_BATCH` run reports
`RecordsAffected` -1 because the server returns only the run's total; the
others report their own count.

There is no transaction around a batch: every statement (and every row of a
merged run) autocommits, and on an error the statements before it stay
applied.

## Transactions

`BeginTransaction()` throws `NotSupportedException`, and so does setting a
non-null `Transaction` on a command or batch: the wire protocol has no
transactions and every statement autocommits.

## Async

The driver's socket I/O is synchronous. `OpenAsync`, `ExecuteReaderAsync`,
`ExecuteNonQueryAsync`, `ExecuteScalarAsync`, `ReadAsync` and the rest are
the ADO.NET base implementations, which run the synchronous method and
return a completed task. They are safe to call; they do not free the thread
during I/O.

## Dapper

Dapper works as-is, with `@name` parameters:

```csharp
using Dapper;

var people = conn.Query<Person>("SELECT id, name FROM people WHERE id >= @min", new { min = 1L });
conn.Execute("INSERT INTO people (id, name) VALUES (@Id, @Name)", listOfPeople);
long n = conn.ExecuteScalar<long>("SELECT count(*) FROM people");
```

Map skaidb `Int` columns to `long` properties (values arrive as `long`)
and timestamps to `DateTimeOffset` properties.
