// Typed parameters of every kind through a prepared statement, then a bulk
// insert in ONE round-trip with ExecuteBatch.
//
//   dotnet run --project examples/PreparedBatch -- [host] [port] [user] [password] [database]

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
    cmd.CommandText = "DROP TABLE IF EXISTS events";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE TABLE events (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}

// One row with every type the codec knows. Arrays and documents have no SQL
// literal form: they only get through as typed parameters, which is what a
// command with Parameters uses (the statement is prepared on the server once
// per connection and executed with the values).
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "INSERT INTO events (id, kind, score, ok, note, tags, payload, amount, ref, at, raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    cmd.Parameters.Add(1L);                                              // Int
    cmd.Parameters.Add("click");                                         // String
    cmd.Parameters.Add(0.75);                                            // Float
    cmd.Parameters.Add(true);                                            // Bool
    cmd.Parameters.Add(null);                                            // Null
    cmd.Parameters.Add(new[] { "web", "eu" });                           // Array
    cmd.Parameters.Add(new Dictionary<string, object?> { ["page"] = "/", ["ms"] = 12L });   // Document
    cmd.Parameters.Add(19.99m);                                          // Decimal (exact)
    cmd.Parameters.Add(Guid.NewGuid());                                  // Uuid
    cmd.Parameters.Add(DateTimeOffset.UtcNow);                           // Timestamp (ms precision)
    cmd.Parameters.Add(new byte[] { 0xde, 0xad, 0xbe, 0xef });           // Bytes
    Console.WriteLine($"typed insert: affected={cmd.ExecuteNonQuery()}");
}

// Bulk: one prepared statement, many parameter rows, one request. Rows
// autocommit individually — on the first failing row the server reports
// which one and how many applied before it, and those stay applied.
var rows = new List<IReadOnlyList<object?>>();
for (long i = 2; i <= 1001; i++)
{
    rows.Add(new object?[]
    {
        i, i % 2 == 0 ? "view" : "click", i / 1000.0, i % 3 == 0, null,
        new[] { "web", $"u{i % 7}" },
        new Dictionary<string, object?> { ["i"] = i },
        (decimal)i / 100, Guid.NewGuid(), DateTimeOffset.UtcNow.AddSeconds(-i), Array.Empty<byte>(),
    });
}
using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "INSERT INTO events (id, kind, score, ok, note, tags, payload, amount, ref, at, raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    long affected = cmd.ExecuteBatch(rows);
    Console.WriteLine($"batch inserted {affected} rows in one round-trip");
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, kind, score, ok, tags, payload, amount, ref, at FROM events WHERE kind = ? ORDER BY id LIMIT 3";
    cmd.Parameters.Add("click");
    using var r = cmd.ExecuteReader();
    while (r.Read())
    {
        var tags = (object?[])r.GetValue(4);
        var payload = (Dictionary<string, object?>)r.GetValue(5);
        Console.WriteLine($"  {r.GetInt64(0)} {r.GetString(1)} {r.GetDouble(2)} {r.GetBoolean(3)} [{string.Join(",", tags)}] {{{string.Join(",", payload.Keys)}}} {r.GetDecimal(6)} {r.GetGuid(7)} {r.GetDateTimeOffset(8):O}");
    }
    cmd.Parameters.Clear();
    cmd.CommandText = "SELECT count(*) FROM events";
    Console.WriteLine($"count: {cmd.ExecuteScalar()}");
    cmd.CommandText = "DROP TABLE events";
    cmd.ExecuteNonQuery();
}
