// Stream a large result set row by row with bounded memory, and stop early.
//
//   dotnet run --project examples/Stream -- [host] [port] [user] [password] [database]
//
// The abandon rule: the connection is busy until the enumeration is
// exhausted or DISPOSED. `foreach` disposes on every exit — break, return,
// exception — and the driver then drains what is left (or drops the
// connection if that is cheaper) so the next statement reads its own reply.
// Never let a hand-rolled enumerator go out of scope without disposing it.

using System;
using System.Collections.Generic;
using System.Globalization;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";
string database = args.Length > 4 ? args[4] : "";

using var conn = new SkaidbConnection(host: host, port: port, user: user, password: password, database: database);
conn.Open();

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP TABLE IF EXISTS readings";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE TABLE readings (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
    var rows = new List<IReadOnlyList<object?>>();
    for (long i = 1; i <= 5000; i++) rows.Add(new object?[] { i, Math.Sin(i / 100.0) });
    cmd.CommandText = "INSERT INTO readings (id, v) VALUES (?, ?)";
    cmd.ExecuteBatch(rows);
}

// Full scan, one chunk in memory at a time. Name the columns: on a cluster a
// bare `SELECT *` only streams page by page at consistency ONE.
long n = 0;
double sum = 0;
foreach (var row in conn.Stream("SELECT id, v FROM readings"))
{
    n++;
    sum += (double)row[1]!;
}
Console.WriteLine($"columns [{string.Join(", ", conn.StreamColumns)}] rows {n} mean {sum / n}");

// Stop early: `break` leaves the foreach, which disposes the enumerator.
foreach (var row in conn.Stream("SELECT id, v FROM readings", SkaidbConsistency.One))
{
    if ((long)row[0]! >= 3) break;
}

// The connection is at a request boundary again.
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT count(*) FROM readings";
    Console.WriteLine($"count after the abandoned stream: {cmd.ExecuteScalar()}");
    cmd.CommandText = "DROP TABLE readings";
    cmd.ExecuteNonQuery();
}
