// Follow a stream: CREATE STREAM on a table, then Subscribe() yields every
// change as it is captured, forever, with a resumable cursor.
//
//   dotnet run --project examples/Subscribe -- [host] [port] [user] [password] [database]
//
// Subscribe() polls the stream's log (_stream_<name>) with a keyset cursor.
// Each event's Id is its position — an opaque string that sorts in log
// order. Save the last Id you handled and pass it as `after` to resume
// exactly there, across restarts. For push delivery subscribe to
// $stream/<db>/<name> with any MQTT client instead; the events are identical.

using System;
using System.Globalization;
using System.Threading;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";
string database = args.Length > 4 ? args[4] : "";

string cs = $"Host={host};Port={port};User={user};Password={password};Database={database}";
using var conn = new SkaidbConnection(cs);
conn.Open();

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP STREAM IF EXISTS big_orders";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "DROP TABLE IF EXISTS orders";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE TABLE orders (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE STREAM big_orders ON orders WHEN (total > 100)";
    cmd.ExecuteNonQuery();
}

// A writer on a SECOND connection: the subscribing connection is busy
// polling inside the foreach below.
var writer = new Thread(() =>
{
    using var w = new SkaidbConnection(cs);
    w.Open();
    for (long i = 1; i <= 5; i++)
    {
        Thread.Sleep(300);
        using var cmd = w.CreateCommand();
        cmd.CommandText = "INSERT INTO orders (id, total) VALUES (?, ?)";
        cmd.Parameters.Add(i);
        cmd.Parameters.Add(i * 60.0);       // 60, 120, 180, 240, 300: four pass the filter
        cmd.ExecuteNonQuery();
    }
});
writer.Start();

string? lastId = null;
int seen = 0;
foreach (var ev in conn.Subscribe("big_orders", after: lastId, pollMs: 200))
{
    Console.WriteLine($"  {ev.Id}  {ev.Op}  key={ev.Key}  doc={(ev.Doc is null ? "null" : "{...}")}");
    lastId = ev.Id;                          // persist this to resume later
    if (++seen == 4) break;                  // break disposes the enumerator and ends the poll loop
}
writer.Join();

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP STREAM big_orders";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "DROP TABLE orders";
    cmd.ExecuteNonQuery();
}
Console.WriteLine($"done; resume later with after=\"{lastId}\"");
