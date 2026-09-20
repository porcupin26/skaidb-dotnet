// Live end-to-end test against a real skaidb node. Every table it touches is
// named dotnet_* and dropped at the end.
//
//   SKAIDB_LIVE=127.0.0.1:7300 SKAIDB_USER=admin SKAIDB_PASSWORD=adminpw \
//   SKAIDB_DATABASE=default dotnet run --project tests/Live
//
// Without SKAIDB_LIVE it prints "skipped" and exits 0, so CI (which has no
// server) runs it green. Any failure exits 1.
//
// Covers: connect + SCRAM, CREATE TABLE, a prepared INSERT with every typed
// parameter, SELECT with a bound WHERE and ORDER BY reading every type back,
// UPDATE row counts, a one-round-trip batch, a streamed result of 2500 rows,
// a server error surfacing as SkaidbException, and the drivers-table row the
// Hello frame creates (client_name dotnet, client_version = package version).

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Skaidb;

string? live = Environment.GetEnvironmentVariable("SKAIDB_LIVE");
if (string.IsNullOrEmpty(live))
{
    Console.WriteLine("skipped: SKAIDB_LIVE is not set (e.g. SKAIDB_LIVE=127.0.0.1:7300)");
    return 0;
}
int colon = live.LastIndexOf(':');
string host = colon > 0 ? live.Substring(0, colon) : live;
int port = colon > 0 ? int.Parse(live.Substring(colon + 1), CultureInfo.InvariantCulture) : 7000;
string user = Environment.GetEnvironmentVariable("SKAIDB_USER") ?? "anonymous";
string password = Environment.GetEnvironmentVariable("SKAIDB_PASSWORD") ?? "";
string database = Environment.GetEnvironmentVariable("SKAIDB_DATABASE") ?? "";

int failures = 0;
void Check(bool ok, string what)
{
    Console.WriteLine((ok ? "  ok   " : "  FAIL ") + what);
    if (!ok) failures++;
}
int Exec(SkaidbConnection c, string sql, params object?[] args)
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = sql;
    foreach (var a in args) cmd.Parameters.Add(a);
    return cmd.ExecuteNonQuery();
}

var connString = $"Host={host};Port={port};User={user};Password={password};Database={database};Consistency=Quorum;Timeout=30";
using var conn = new SkaidbConnection(connString);
conn.Open();
Console.WriteLine($"connected to {host}:{port} as {user} (driver {SkaidbConnection.DriverName} {SkaidbConnection.DriverVersion})");
Check(conn.IsOpen && conn.IsUsable, "connect + SCRAM-SHA-256");

const string T = "dotnet_types";
const string BIG = "dotnet_big";
try
{
    Exec(conn, $"DROP TABLE IF EXISTS {T}");
    Exec(conn, $"DROP TABLE IF EXISTS {BIG}");
    Exec(conn, $"CREATE TABLE {T} (PRIMARY KEY (id))");
    Check(true, "CREATE TABLE");

    // ---- prepared INSERT with every typed parameter ------------------------
    var uuid = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
    var ts = new DateTimeOffset(2026, 9, 19, 12, 34, 56, 789, TimeSpan.Zero);
    var bytes = new byte[] { 0x00, 0x01, 0xfe, 0xff, 0x7f };
    var arr = new object?[] { 1L, "two", 3.5, true, null };
    var doc = new Dictionary<string, object?> { ["name"] = "Ada", ["n"] = 42L, ["tags"] = new object?[] { "x", "y" } };
    decimal dec = 12345.6789m;

    int n = Exec(conn,
        $"INSERT INTO {T} (id, s, f, b, nul, arr, doc, dec, u, ts, raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        1L, "O'Brien ünïcödé", 2.5, true, null, arr, doc, dec, uuid, ts, bytes);
    Check(n == 1, $"prepared INSERT with 11 typed parameters (affected={n})");
    n = Exec(conn, $"INSERT INTO {T} (id, s, f, b) VALUES (?, ?, ?, ?)", 2L, "second", -1.25, false);
    Check(n == 1, "second INSERT");
    n = Exec(conn, $"INSERT INTO {T} (id, s, f, b) VALUES (?, ?, ?, ?)", 3L, "third", 0.0, true);
    Check(n == 1, "third INSERT");

    // ---- SELECT with a bound WHERE and ORDER BY, every type back -----------
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"SELECT id, s, f, b, nul, arr, doc, dec, u, ts, raw FROM {T} WHERE id <= ? ORDER BY id DESC";
        cmd.Parameters.Add(2L);
        using var r = cmd.ExecuteReader();
        Check(r.FieldCount == 11, $"FieldCount {r.FieldCount}");
        Check(r.Read() && r.GetInt64(0) == 2, "ORDER BY id DESC: first row is id 2");
        Check(r.Read() && r.GetInt64(0) == 1, "second row is id 1");
        Check(r.GetString(1) == "O'Brien ünïcödé", $"string round-trip: {r.GetString(1)}");
        Check(r.GetDouble(2) == 2.5, $"float round-trip: {r.GetDouble(2)}");
        Check(r.GetBoolean(3), "bool round-trip");
        Check(r.IsDBNull(4) && r.GetValue(4) == DBNull.Value, "null round-trip (IsDBNull, DBNull.Value)");
        var gotArr = r.GetValue(5) as object?[];
        Check(gotArr is not null && gotArr.Length == 5 && (long)gotArr[0]! == 1 && (string)gotArr[1]! == "two"
              && (double)gotArr[2]! == 3.5 && (bool)gotArr[3]! && gotArr[4] is null,
              "array round-trip: " + (gotArr is null ? r.GetValue(5).GetType().Name : $"[{string.Join(", ", gotArr)}]"));
        var gotDoc = r.GetValue(6) as Dictionary<string, object?>;
        Check(gotDoc is not null && (string)gotDoc["name"]! == "Ada" && (long)gotDoc["n"]! == 42
              && gotDoc["tags"] is object?[] tg && tg.Length == 2 && (string)tg[1]! == "y",
              "document round-trip: " + (gotDoc is null ? r.GetValue(6).GetType().Name : string.Join(", ", gotDoc.Select(kv => kv.Key + "=" + kv.Value))));
        object decVal = r.GetValue(7);
        Check(decVal is decimal dd && dd == dec, $"decimal round-trip: {decVal} ({decVal.GetType().Name})");
        Check(r.GetDecimal(7) == dec, "GetDecimal");
        Check(r.GetGuid(8) == uuid, $"uuid round-trip: {r.GetGuid(8)}");
        Check(r.GetDateTimeOffset(9) == ts, $"timestamp round-trip: {r.GetDateTimeOffset(9):O}");
        Check(r.GetBytes(10).SequenceEqual(bytes), $"bytes round-trip: {BitConverter.ToString(r.GetBytes(10))}");
        Check(!r.Read(), "no third row (WHERE id <= 2)");
        Check(r.GetOrdinal("DOC") == 6 && r.GetName(6) == "doc", "GetOrdinal / GetName");
    }

    // ---- UPDATE row count --------------------------------------------------
    n = Exec(conn, $"UPDATE {T} SET s = ? WHERE id > ?", "renamed", 1L);
    Check(n == 2, $"UPDATE affected {n} (expected 2)");
    n = Exec(conn, $"UPDATE {T} SET s = ? WHERE id = ?", "nobody", 999L);
    Check(n == 0, $"UPDATE of a missing row affected {n} (expected 0)");
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"SELECT count(*) FROM {T} WHERE s = ?";
        cmd.Parameters.Add("renamed");
        object? cnt = cmd.ExecuteScalar();
        Check(cnt is long l && l == 2, $"ExecuteScalar count = {cnt}");
    }
    n = Exec(conn, $"DELETE FROM {T} WHERE id = ?", 3L);
    Check(n == 1, $"DELETE affected {n}");

    // ---- batch (one round-trip) ---------------------------------------------
    Exec(conn, $"CREATE TABLE {BIG} (PRIMARY KEY (id))");
    const int ROWS = 2500;
    var batch = new List<IReadOnlyList<object?>>(ROWS);
    for (int i = 1; i <= ROWS; i++)
        batch.Add(new object?[] { (long)i, i * 0.5, $"row-{i}", new Dictionary<string, object?> { ["i"] = (long)i } });
    long total;
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"INSERT INTO {BIG} (id, v, name, meta) VALUES (?, ?, ?, ?)";
        total = cmd.ExecuteBatch(batch);
    }
    Check(total == ROWS, $"ExecuteBatch inserted {total} rows in one round-trip");

    // ---- streamed result ----------------------------------------------------
    long streamed = 0, idSum = 0;
    double vSum = 0;
    foreach (var row in conn.Stream($"SELECT id, v, name FROM {BIG}"))
    {
        streamed++;
        idSum += (long)row[0]!;
        vSum += (double)row[1]!;
    }
    Check(conn.StreamColumns.SequenceEqual(new[] { "id", "v", "name" }), $"stream columns: {string.Join(",", conn.StreamColumns)}");
    Check(streamed == ROWS, $"streamed {streamed} rows");
    Check(idSum == (long)ROWS * (ROWS + 1) / 2, $"stream id sum {idSum}");
    Check(Math.Abs(vSum - 0.5 * ROWS * (ROWS + 1) / 2) < 1e-6, $"stream v sum {vSum}");
    // Abandon a stream early: the connection must still be usable afterwards.
    int peeked = 0;
    foreach (var row in conn.Stream($"SELECT id, v FROM {BIG}")) { if (++peeked >= 3) break; }
    Check(peeked == 3, "stream abandoned after 3 rows");
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"SELECT count(*) FROM {BIG}";
        Check(cmd.ExecuteScalar() is long c2 && c2 == ROWS, "connection usable after an abandoned stream");
    }
    // A non-streaming SELECT of the whole table, for comparison.
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"SELECT id FROM {BIG} WHERE id > ? ORDER BY id LIMIT 5";
        cmd.Parameters.Add((long)(ROWS - 5));
        using var r = cmd.ExecuteReader();
        var ids = new List<long>();
        while (r.Read()) ids.Add(r.GetInt64(0));
        Check(ids.SequenceEqual(new long[] { ROWS - 4, ROWS - 3, ROWS - 2, ROWS - 1, ROWS }), $"ORDER BY + LIMIT: {string.Join(",", ids)}");
    }

    // ---- errors -------------------------------------------------------------
    try
    {
        Exec(conn, "SELECT nope FROM dotnet_no_such_table");
        Check(false, "error did not throw");
    }
    catch (SkaidbException e)
    {
        Check(true, $"server error is a SkaidbException: {e.Message}");
    }
    Check(conn.IsUsable, "connection usable after a statement error");
    try
    {
        Exec(conn, $"SELECT id FROM {T} WHERE id = ?", 1L, 2L);
        Check(false, "arity mismatch did not throw");
    }
    catch (SkaidbException e)
    {
        Check(e.Message.Contains("parameters"), $"arity mismatch is a SkaidbException: {e.Message}");
    }
    try
    {
        Exec(conn, $"INSERT INTO {T} (id, f) VALUES (?, ?)", 4L, double.NaN);
        Check(false, "NaN did not throw");
    }
    catch (SkaidbException e)
    {
        Check(e.Message.Contains("NaN"), $"NaN refused client-side: {e.Message}");
    }

    // ---- multiple result sets: a plain SELECT has none ---------------------
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = $"SELECT id FROM {T}";
        using var r = cmd.ExecuteReader();
        Check(!r.NextResult(), "NextResult() is false for a single result set");
    }

    // ---- the drivers table ---------------------------------------------------
    Thread.Sleep(1000);
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "SELECT client_name, client_version FROM drivers";
        using var r = cmd.ExecuteReader();
        bool found = false;
        var seen = new StringBuilder();
        while (r.Read())
        {
            string cn = r.IsDBNull(0) ? "" : r.GetString(0);
            string cv = r.IsDBNull(1) ? "" : r.GetString(1);
            seen.Append($"[{cn} {cv}] ");
            if (cn == SkaidbConnection.DriverName && cv == SkaidbConnection.DriverVersion) found = true;
        }
        Check(found, $"drivers table has {SkaidbConnection.DriverName} / {SkaidbConnection.DriverVersion}; rows: {seen}");
    }
}
finally
{
    try { Exec(conn, $"DROP TABLE IF EXISTS {T}"); } catch (Exception e) { Console.WriteLine("cleanup: " + e.Message); }
    try { Exec(conn, $"DROP TABLE IF EXISTS {BIG}"); } catch (Exception e) { Console.WriteLine("cleanup: " + e.Message); }
    Console.WriteLine("dropped dotnet_* tables");
}

Console.WriteLine(failures == 0 ? "LIVE: all checks passed" : $"LIVE: {failures} check(s) FAILED");
return failures == 0 ? 0 : 1;
