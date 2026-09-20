// Connect, create a table, insert with bound parameters, select, clean up.
//
//   dotnet run --project examples/Basic -- [host] [port] [user] [password] [database]
//
// In your own project: dotnet add package Skaidb  (then `using Skaidb;`).

using System;
using System.Globalization;
using Skaidb;

string host = args.Length > 0 ? args[0] : "localhost";
int port = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 7000;
string user = args.Length > 2 ? args[2] : "anonymous";
string password = args.Length > 3 ? args[3] : "";
string database = args.Length > 4 ? args[4] : "";

using var conn = new SkaidbConnection(
    $"Host={host};Port={port};User={user};Password={password};Database={database};Consistency=Quorum");
conn.Open();   // TCP connect + SCRAM-SHA-256 handshake
Console.WriteLine($"Connected to {host}:{port} as {user} (driver {SkaidbConnection.DriverVersion}).");

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "DROP TABLE IF EXISTS people";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "CREATE TABLE people (PRIMARY KEY (id))";
    cmd.ExecuteNonQuery();
}

// `?` placeholders, bound positionally from cmd.Parameters. With parameters
// the statement is prepared on the server and the values travel typed.
foreach (var (id, name, age, tags) in new[]
{
    (1L, "Ada", 36L, new[] { "math", "engines" }),
    (2L, "Grace", 45L, new[] { "compilers" }),
    (3L, "O'Brien", 52L, Array.Empty<string>()),   // apostrophes are never a problem
})
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "INSERT INTO people (id, name, age, tags) VALUES (?, ?, ?, ?)";
    cmd.Parameters.Add(id);
    cmd.Parameters.Add(name);
    cmd.Parameters.Add(age);
    cmd.Parameters.Add(tags);        // an array: only a typed parameter can carry one
    Console.WriteLine($"inserted {name}: affected={cmd.ExecuteNonQuery()}");
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT id, name, age, tags FROM people WHERE age >= ? ORDER BY id";
    cmd.Parameters.Add(40L);
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        var tags = (object?[])reader.GetValue(3);
        Console.WriteLine($"  {reader.GetInt64(0)}  {reader.GetString(1)}  {reader.GetInt64(2)}  [{string.Join(", ", tags)}]");
    }
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "SELECT name FROM people WHERE id = ?";
    cmd.Parameters.Add(3L);
    Console.WriteLine($"scalar: {cmd.ExecuteScalar()}");
}

using (var cmd = conn.CreateCommand())
{
    cmd.CommandText = "UPDATE people SET age = age + 1 WHERE id > ?";
    cmd.Parameters.Add(1L);
    Console.WriteLine($"updated {cmd.ExecuteNonQuery()} rows");
    cmd.CommandText = "DROP TABLE people";
    cmd.Parameters.Clear();
    cmd.ExecuteNonQuery();
}
Console.WriteLine("Done.");
