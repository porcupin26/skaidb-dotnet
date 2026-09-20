// A bounded pool of connections shared by several threads.
//
//   dotnet run --project examples/Pool -- [host] [port] [user] [password] [database]
//
// maxsize bounds the connections kept IDLE, not the number checked out: a
// burst opens extras and the surplus is disposed on release. Pooled
// connections inherit seed failover, TLS and the session database from the
// connection string.

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";
string database = args.Length > 4 ? args[4] : "";

using var pool = new SkaidbConnectionPool(
    $"Host={host};Port={port};User={user};Password={password};Database={database}", maxsize: 4);

pool.WithConnection(c =>
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = "DROP TABLE IF EXISTS hits";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE TABLE hits (PRIMARY KEY (id))";
    return cmd.ExecuteNonQuery();
});

// Eight workers, four idle connections at most: each worker checks one out
// for the duration of its statement and hands it back.
int inserted = 0;
Parallel.For(0, 8, worker =>
{
    for (int i = 0; i < 25; i++)
    {
        pool.WithConnection(c =>
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "INSERT INTO hits (id, worker) VALUES (?, ?)";
            cmd.Parameters.Add((long)(worker * 100 + i));
            cmd.Parameters.Add((long)worker);
            return cmd.ExecuteNonQuery();
        });
        Interlocked.Increment(ref inserted);
    }
});
Console.WriteLine($"inserted {inserted} rows through the pool");

// Acquire/Release by hand when a callback does not fit.
var conn = pool.Acquire();
try
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT count(*) FROM hits";
    Console.WriteLine($"count: {cmd.ExecuteScalar()}");
    cmd.CommandText = "DROP TABLE hits";
    cmd.ExecuteNonQuery();
}
finally
{
    pool.Release(conn);
}
