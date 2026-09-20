# Getting started

## Requirements

- .NET 8.0 SDK or newer.
- A reachable skaidb node (default client port **7000**). Any node of a
  cluster will do: skaidb is leaderless.

The driver has no dependencies: one assembly, pure BCL.

## Install

From the GitHub release (the package is not on NuGet.org yet; every release
at <https://github.com/porcupin26/skaidb-dotnet/releases> attaches
`Skaidb.X.Y.Z.nupkg`):

```sh
V=1.0.1
mkdir -p ~/nuget-local
curl -L -o ~/nuget-local/Skaidb.$V.nupkg \
  https://github.com/porcupin26/skaidb-dotnet/releases/download/v$V/Skaidb.$V.nupkg
dotnet nuget add source ~/nuget-local --name skaidb-local
dotnet add package Skaidb --version $V
```

From source, as a project reference (a clone or a git submodule):

```sh
git clone https://github.com/porcupin26/skaidb-dotnet.git
dotnet add reference path/to/skaidb-dotnet/Skaidb.csproj
```

Either way the namespace is `Skaidb`.

## Connect and query

```csharp
using System;
using Skaidb;

using var conn = new SkaidbConnection(
    "Host=localhost;Port=7000;User=skaidb;Password=secret;Database=app");
conn.Open();

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "CREATE TABLE IF NOT EXISTS users (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "INSERT INTO users (id, name, tags) VALUES (?, ?, ?)";
    cmd.Parameters.Add(1L);
    cmd.Parameters.Add("Ada");
    cmd.Parameters.Add(new[] { "admin" });
    cmd.ExecuteNonQuery();
}
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name, tags FROM users WHERE id = ?";
    cmd.Parameters.Add(1L);
    using var r = cmd.ExecuteReader();
    while (r.Read())
    {
        var tags = (object?[])r.GetValue(2);
        Console.WriteLine($"{r.GetInt64(0)} {r.GetString(1)} [{string.Join(",", tags)}]");
    }
}
```

Placeholders are `?`, bound in order from `cmd.Parameters`. With parameters
the statement is prepared on the server and the values are sent typed — that
is how the array above gets through; it has no SQL literal form.

`Database=app` runs `USE app` right after the handshake (and after every
reconnect). Without it, statements run in the server's default database.

## Anonymous connections

A server with authentication disabled accepts `User=anonymous` (the default)
with an empty password. The SCRAM handshake still runs; the server verifies
nothing, and the driver skips mutual authentication when the password is
empty.

## TLS

A server with `client_tls = required` refuses plaintext, so one of these is
needed there:

```csharp
new SkaidbConnection("...;TlsCa=/etc/skaidb/skai-ca.crt");   // verify against your CA — recommended
new SkaidbConnection("...;Tls=true");                          // verify against the system trust store
new SkaidbConnection("...;TlsInsecure=true");                  // encrypt only, no verification — development
```

`TlsServerName` (default `skaidb`) is the SNI name and the name the
certificate is verified against. It must match a SAN on the server
certificate — which is normally *not* the host you dialled; skaidb's own
certificates carry `DNS:skaidb`. See [tls.md](tls.md).

## Several nodes

```csharp
new SkaidbConnection("Seeds=db1:7000,db2:7000,db3:7000;User=app;Password=secret");
```

The seeds are shuffled and tried until one connects. The same walk runs when
a connection is lost later: the next statement on the connection re-dials,
re-authenticates, re-selects the database, and runs. A statement that was in
flight when the connection died throws and is never retried by the driver
(it may have executed).

## Many concurrent statements

Statements on one connection run one at a time. For concurrency use a pool:

```csharp
using var pool = new SkaidbConnectionPool("Seeds=db1:7000,db2:7000;User=app;Password=secret", maxsize: 8);
long n = pool.WithConnection(c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "SELECT count(*) FROM users";
    return (long)cmd.ExecuteScalar()!;
});
```

See [pooling.md](pooling.md).

## Big results

```csharp
foreach (var row in conn.Stream("SELECT id, name FROM users"))
{
    // one row at a time, one chunk in memory
}
```

Read the [abandon rule](streaming.md#the-abandon-rule) before stopping a
stream early: the connection is busy until the enumerator is disposed.

## Bulk inserts

```csharp
cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
cmd.ExecuteBatch(new List<IReadOnlyList<object?>> { new object?[] { 1L, "Ada" }, new object?[] { 2L, "Linus" } });
```

One round-trip for all rows; each row autocommits on its own.

## Next

- [API reference](api.md)
- [Types](types.md)
- [Examples](../examples/): `Basic`, `PreparedBatch`, `Stream`, `Tls`, `Pool`, `Subscribe` — each is
  `dotnet run --project examples/<Name> -- host port user password database`.
