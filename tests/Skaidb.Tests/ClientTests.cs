// The connection end to end against the in-process fake server: handshake,
// Hello, every request opcode, streaming (including the abandon/drain rule),
// batches, failover, reconnect, the pool and Subscribe.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Xunit;

namespace Skaidb.Tests;

public class ClientTests
{
    private static SkaidbConnection Connect(FakeServer srv, string extra = "")
    {
        var c = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5;{extra}");
        c.Open();
        return c;
    }

    private static List<object?[]> All(SkaidbDataReader r)
    {
        var rows = new List<object?[]>();
        while (r.Read())
        {
            var row = new object?[r.FieldCount];
            for (int i = 0; i < r.FieldCount; i++) row[i] = r.IsDBNull(i) ? null : r.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    private static object? Scalar(SkaidbConnection c, string sql, params object?[] p)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in p) cmd.Parameters.Add(v);
        return cmd.ExecuteScalar();
    }

    [Fact]
    public void Open_runs_SCRAM_verifies_the_server_signature_and_sends_Hello()
    {
        using var srv = new FakeServer();
        using var c = Connect(srv);
        Assert.True(c.IsOpen);
        Assert.True(c.IsUsable);
        Assert.Single(srv.Hellos);
        Assert.Equal("dotnet", srv.Hellos[0].Name);
        Assert.Equal(SkaidbConnection.DriverVersion, srv.Hellos[0].Version);
        // The first frame on the wire is AuthStart with a big-endian length.
        byte[] first = srv.FirstFrames[0];
        int len = (first[0] << 24) | (first[1] << 16) | (first[2] << 8) | first[3];
        Assert.Equal(first.Length - 4, len);
        Assert.Equal(10, first[4]);
        var r = new BinReader(first.AsSpan(5).ToArray());
        Assert.Equal("ada", r.Text());
        Assert.StartsWith("cs", r.Text());
        c.Open();                                   // idempotent
        Assert.Single(srv.Hellos);
        c.Close();
        Assert.False(c.IsUsable);
        Assert.Throws<ObjectDisposedException>(() => c.Open());
    }

    [Fact]
    public void Wrong_password_is_denied_and_a_forged_server_signature_is_refused()
    {
        using var srv = new FakeServer();
        using var c = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=nope");
        var e = Assert.Throws<SkaidbException>(() => c.Open());
        Assert.Contains("authentication denied: bad password", e.Message);
        Assert.False(c.IsOpen);
        srv.CorruptServerSignature = true;
        using var d = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret");
        Assert.Contains("server signature mismatch", Assert.Throws<SkaidbException>(() => d.Open()).Message);
    }

    [Fact]
    public void Query_decodes_Rows_Mutation_Ddl_and_Error_with_per_command_consistency()
    {
        var s = new Scripted()
            .Answer("SELECT id, name FROM t", F.Rows(new[] { "id", "name" }, new object?[] { 1L, "Ada" }, new object?[] { 2L, null }))
            .Answer("INSERT INTO t VALUES (1)", F.Mutation(3))
            .Answer("CREATE TABLE t (PRIMARY KEY (id))", F.Ddl())
            .Answer("SELECT nope", F.Error("no such column nope"))
            .Answer("SELECT 1 AT LEVEL", req => F.Rows(new[] { "c" }, new object?[] { (long)req.Consistency }));
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv, "Consistency=One");

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT id, name FROM t";
            using var r = cmd.ExecuteReader();
            Assert.Equal(2, r.FieldCount);
            Assert.Equal("name", r.GetName(1));
            Assert.Equal(1, r.GetOrdinal("name"));
            Assert.Equal(1, r.GetOrdinal("NAME"));
            Assert.Throws<SkaidbException>(() => r.GetOrdinal("zzz"));
            Assert.Throws<SkaidbException>(() => r.GetValue(0));       // before Read()
            Assert.True(r.Read());
            Assert.Equal(1L, r.GetInt64(0));
            Assert.Equal(1, r.GetInt32(0));
            Assert.Equal("Ada", r.GetString(1));
            Assert.Equal("Ada", r["name"]);
            Assert.Equal(1L, r[0]);
            Assert.True(r.Read());
            Assert.True(r.IsDBNull(1));
            Assert.Equal(DBNull.Value, r.GetValue(1));
            Assert.Throws<SkaidbException>(() => r.GetString(1));
            Assert.False(r.Read());
            Assert.Throws<SkaidbException>(() => r.GetValue(0));       // past the end
            Assert.False(r.NextResult());
        }
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t VALUES (1)";
            Assert.Equal(3, cmd.ExecuteNonQuery());
            using var r = cmd.ExecuteReader();                          // a non-row result: empty reader
            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
            Assert.Null(cmd.ExecuteScalar());
        }
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (PRIMARY KEY (id))";
            Assert.Equal(0, cmd.ExecuteNonQuery());
        }
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT nope";
            var e = Assert.Throws<SkaidbException>(() => cmd.ExecuteReader());
            Assert.Equal("no such column nope", e.Message);
        }
        Assert.True(c.IsUsable);                                         // a statement error keeps the connection
        Assert.Equal(0L, Scalar(c, "SELECT 1 AT LEVEL"));                // connection default One
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT 1 AT LEVEL";
            cmd.Consistency = SkaidbConsistency.All;
            Assert.Equal(2L, cmd.ExecuteScalar());
        }
        c.Consistency = SkaidbConsistency.Quorum;
        Assert.Equal(1L, Scalar(c, "SELECT 1 AT LEVEL"));
        Assert.All(srv.Requests.Where(q => q.Op == 1), q => Assert.NotEmpty(q.Sql));
    }

    [Fact]
    public void Parameters_go_through_server_side_prepare_with_typed_values_cached_per_connection()
    {
        var executed = new List<Request>();
        const string sql = "SELECT * FROM t WHERE id = ? AND tags = ? AND meta = ?";
        var s = new Scripted().Prepare(sql, 3);
        s.OnExecute = (stmt, req) => { executed.Add(req); return new[] { F.Rows(new[] { "ok" }, new object?[] { true }) }; };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv, "Consistency=All");
        var doc = new Dictionary<string, object?> { ["k"] = DateTimeOffset.FromUnixTimeMilliseconds(5), ["n"] = null, ["b"] = new byte[] { 0x7a } };
        Assert.Equal(true, Scalar(c, sql, 1L << 62, new object?[] { "a", "b" }, doc));
        Assert.Equal(true, Scalar(c, sql, 1, Array.Empty<object>(), new Dictionary<string, object?>()));
        Assert.Single(srv.Requests.Where(q => q.Op == 2));                // prepared once
        Assert.Equal(2, executed.Count);
        Assert.Equal(2, executed[0].Consistency);
        Assert.Equal(1L << 62, executed[0].Params[0]);
        Assert.Equal(new object?[] { "a", "b" }, (object?[])executed[0].Params[1]!);
        var gotDoc = (Dictionary<string, object?>)executed[0].Params[2]!;
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(5), gotDoc["k"]);
        Assert.Null(gotDoc["n"]);
        Assert.Equal(new byte[] { 0x7a }, (byte[])gotDoc["b"]!);
        Assert.Equal(1L, executed[1].Params[0]);
        var e = Assert.Throws<SkaidbException>(() => Scalar(c, sql, 1, 2));
        Assert.Equal("statement expects 3 parameters, got 2", e.Message);
        Assert.Equal(2, executed.Count);                                 // nothing was sent
    }

    [Fact]
    public void An_unpreparable_statement_falls_back_to_client_side_literals()
    {
        var seen = new List<string>();
        using var srv = new FakeServer((req, conn) =>
        {
            if (req.Op == 1) { seen.Add(req.Sql); return new[] { F.Ddl() }; }
            if (req.Op == 2) return new[] { F.Error("statement kind cannot be prepared") };
            return null;
        });
        using var c = Connect(srv);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "USE ?";
        cmd.Parameters.Add("app'db");
        Assert.Equal(0, cmd.ExecuteNonQuery());
        Assert.Equal(new[] { "USE 'app''db'" }, seen);
        // The refusal is not cached: the server is asked again next time.
        cmd.ExecuteNonQuery();
        Assert.Equal(2, srv.Requests.Count(q => q.Op == 2));
    }

    [Fact]
    public void A_prepare_that_fails_with_a_real_error_surfaces_that_error_not_a_binding_failure()
    {
        // A parse error on prepare used to be treated as "unpreparable" and
        // retried as text, where an array parameter then failed with
        // "cannot bind value of type Object[]" and hid the real message.
        using var srv = new FakeServer((req, conn) =>
            req.Op == 2 ? new[] { F.Error("unexpected token \"Keyword(By)\", expected identifier") } : null);
        using var c = Connect(srv);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "INSERT INTO t (id, by) VALUES (?, ?)";
        cmd.Parameters.Add(1);
        cmd.Parameters.Add(new object?[] { "x" });
        var e = Assert.Throws<SkaidbException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("Keyword(By)", e.Message);
        Assert.Empty(srv.Requests.Where(q => q.Op == 1));                 // no text retry
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void ResultSets_from_a_CALL_that_EMITs_walk_with_NextResult()
    {
        var s = new Scripted()
            .Answer("CALL report()", F.ResultSets((new[] { "a" }, new object?[][] { new object?[] { 1L }, new object?[] { 2L } }),
                                                  (new[] { "b", "c" }, new object?[][] { new object?[] { "x", "y" } })))
            .Answer("CALL empty()", F.ResultSets());
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "CALL report()";
            using var r = cmd.ExecuteReader();
            Assert.Equal(new[] { "a" }, new[] { r.GetName(0) });
            Assert.Equal(2, All(r).Count);
            Assert.True(r.NextResult());
            Assert.Equal(2, r.FieldCount);
            Assert.True(r.Read());
            Assert.Equal("y", r.GetString(1));
            Assert.False(r.NextResult());
            Assert.Equal(1L, Scalar(c, "CALL report()"));                // the first set's first cell
        }
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "CALL empty()";
            using var r = cmd.ExecuteReader();
            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
        }
    }

    [Fact]
    public void Database_is_selected_with_USE_after_the_handshake_and_after_a_reconnect()
    {
        var seen = new List<string>();
        using var srv = new FakeServer((req, conn) => { if (req.Op == 1) seen.Add(req.Sql); return new[] { F.Ddl() }; });
        using var c = Connect(srv, "Database=my\"db");
        Assert.Equal(new[] { "USE \"my\"\"db\"" }, seen);
        srv.CloseAll();
        Thread.Sleep(50);
        using (var cmd = c.CreateCommand()) { cmd.CommandText = "SELECT 1"; cmd.ExecuteNonQuery(); }
        Assert.Equal(new[] { "USE \"my\"\"db\"", "USE \"my\"\"db\"", "SELECT 1" }, seen);
        Assert.Equal(2, srv.Connections);
        Assert.Equal(2, srv.Hellos.Count);
    }

    [Fact]
    public void Stream_yields_rows_chunk_by_chunk_exposes_columns_and_ends_cleanly()
    {
        var s = new Scripted()
            .Answer("SELECT id FROM big", F.Header("id"), F.Chunk(new object?[] { 1L }, new object?[] { 2L }), F.Chunk(), F.Chunk(new object?[] { 3L }), F.End())
            .Answer("SELECT id FROM t", F.Rows(new[] { "id" }, new object?[] { 9L }))
            .Answer("INSERT INTO t VALUES (1)", F.Mutation(1))
            .Answer("SELECT boom", F.Header("id"), F.Chunk(new object?[] { 1L }), F.Error("scan budget exceeded"))
            .Answer("SELECT early", F.Error("no such table"))
            .Answer("CALL emits()", F.ResultSets((new[] { "a" }, new object?[][] { new object?[] { 1L } })))
            .Answer("SELECT lvl", req => F.Rows(new[] { "c" }, new object?[] { (long)req.Consistency }));
        s.Script["SELECT lvl"] = (req, _) => new[] { F.Header("c"), F.Chunk(new object?[] { (long)req.Consistency }), F.End() };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        var got = c.Stream("SELECT id FROM big").Select(r => (long)r[0]!).ToList();
        Assert.Equal(new[] { 1L, 2L, 3L }, got);
        Assert.Equal(new[] { "id" }, c.StreamColumns);
        Assert.True(c.IsUsable);
        // A non-row statement streamed yields nothing and does not desync.
        Assert.Empty(c.Stream("INSERT INTO t VALUES (1)").ToList());
        Assert.Equal(9L, Scalar(c, "SELECT id FROM t"));
        // Error before the header: a plain statement failure.
        Assert.Equal("no such table", Assert.Throws<SkaidbException>(() => c.Stream("SELECT early").ToList()).Message);
        // Error after the header: rows so far are valid, the connection is fine.
        var partial = new List<long>();
        var e = Assert.Throws<SkaidbException>(() => { foreach (var r in c.Stream("SELECT boom")) partial.Add((long)r[0]!); });
        Assert.Equal("scan budget exceeded", e.Message);
        Assert.Equal(new[] { 1L }, partial);
        Assert.True(c.IsUsable);
        Assert.Equal(9L, Scalar(c, "SELECT id FROM t"));
        // A ResultSets reply on the stream opcode is out of contract: the socket is dropped, the next statement re-dials.
        Assert.Contains("unexpected response tag 8", Assert.Throws<SkaidbException>(() => c.Stream("CALL emits()").ToList()).Message);
        Assert.False(c.IsUsable);
        Assert.Equal(9L, Scalar(c, "SELECT id FROM t"));
        Assert.Equal(2, srv.Connections);
        // Per-stream consistency: the stream request carries it.
        Assert.Equal(2L, c.Stream("SELECT lvl", SkaidbConsistency.All).Single()[0]);
        Assert.Equal(2, srv.Requests.Last(q => q.Op == 5).Consistency);
        Assert.Equal(1L, c.Stream("SELECT lvl").Single()[0]);
        Assert.Equal(1, srv.Requests.Last(q => q.Op == 5).Consistency);
    }

    [Fact]
    public void Abandoning_a_stream_drains_the_rest_so_the_next_statement_reads_its_own_reply()
    {
        var chunks = Enumerable.Range(0, 10).Select(i => F.Chunk(new object?[] { (long)i })).ToList();
        var s = new Scripted()
            .Answer("SELECT id FROM big", new[] { F.Header("id") }.Concat(chunks).Append(F.End()).ToArray())
            .Answer("SELECT 42", F.Rows(new[] { "v" }, new object?[] { 42L }));
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        foreach (var row in c.Stream("SELECT id FROM big")) { if ((long)row[0]! == 1) break; }
        Assert.True(c.IsUsable);                                          // drained, same connection
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        // Explicit Dispose of the enumerator, and a throw out of the loop body.
        using (var it = c.Stream("SELECT id FROM big").GetEnumerator())
        {
            Assert.True(it.MoveNext());
            Assert.False(c.IsUsable);                                     // busy while streaming
            var busy = Assert.Throws<SkaidbException>(() => Scalar(c, "SELECT 42"));
            Assert.Contains("busy streaming", busy.Message);
        }
        Assert.True(c.IsUsable);
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        Assert.Throws<InvalidOperationException>(() => { foreach (var _ in c.Stream("SELECT id FROM big")) throw new InvalidOperationException("mine"); });
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        Assert.Equal(1, srv.Connections);
    }

    [Fact]
    public void Abandoning_a_stream_with_too_much_left_drops_the_connection_and_the_next_statement_redials()
    {
        var chunks = Enumerable.Range(0, 100).Select(i => F.Chunk(new object?[] { (long)i })).ToList();   // > 64-frame drain budget
        var s = new Scripted()
            .Answer("SELECT id FROM big", new[] { F.Header("id") }.Concat(chunks).Append(F.End()).ToArray())
            .Answer("SELECT 42", F.Rows(new[] { "v" }, new object?[] { 42L }));
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        foreach (var _ in c.Stream("SELECT id FROM big")) break;
        Assert.False(c.IsUsable);
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        Assert.Equal(2, srv.Connections);
        Assert.Equal(2, srv.Hellos.Count);                                // Hello again after the reconnect
    }

    [Fact]
    public void Batch_sends_one_OP_EXECUTE_BATCH_with_typed_rows_and_returns_the_total()
    {
        var batches = new List<(string Sql, Request Req)>();
        const string sql = "INSERT INTO t (id, tags) VALUES (?, ?)";
        var s = new Scripted().Prepare(sql, 2);
        s.OnBatch = (stmt, req) => { batches.Add((stmt, req)); return new[] { F.Mutation((ulong)req.Rows.Count) }; };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv, "Consistency=All");
        var rows = new List<IReadOnlyList<object?>>
        {
            new object?[] { 1L, new object?[] { "a" } },
            new object?[] { 2L, Array.Empty<object?>() },
            new object?[] { 3L, new object?[] { "b", "c" } },
        };
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        Assert.Equal(3L, cmd.ExecuteBatch(rows));
        Assert.Single(batches);
        Assert.Equal(sql, batches[0].Sql);
        Assert.Equal(2, batches[0].Req.Consistency);
        Assert.Equal(3, batches[0].Req.Rows.Count);
        Assert.Equal(1L, batches[0].Req.Rows[0][0]);
        Assert.Equal(new object?[] { "b", "c" }, (object?[])batches[0].Req.Rows[2][1]!);
        Assert.Equal(0L, cmd.ExecuteBatch(new List<IReadOnlyList<object?>>()));   // no round-trip
        Assert.Single(batches);
        var e = Assert.Throws<SkaidbException>(() => cmd.ExecuteBatch(new List<IReadOnlyList<object?>> { new object?[] { 1L, new object?[0] }, new object?[] { 2L } }));
        Assert.Equal("batch row expects 2 parameters, got 1", e.Message);
        using var ddl = c.CreateCommand();
        ddl.CommandText = "CREATE TABLE x (PRIMARY KEY (id))";
        var u = Assert.ThrowsAny<SkaidbException>(() => ddl.ExecuteBatch(new List<IReadOnlyList<object?>> { new object?[] { 1L } }));
        Assert.Contains("cannot be prepared", u.Message);
        // A failing row: the server's error names it; the total is not returned.
        s.OnBatch = (stmt, req) => new[] { F.Error("row 1: duplicate key (1 rows applied)") };
        Assert.Contains("row 1", Assert.Throws<SkaidbException>(() => cmd.ExecuteBatch(rows)).Message);
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void Seeds_a_dead_endpoint_is_skipped_and_a_live_one_wins()
    {
        var dead = new TcpListener(IPAddress.Loopback, 0);
        dead.Start();
        int deadPort = ((IPEndPoint)dead.LocalEndpoint).Port;
        dead.Stop();                                                       // refuses connections now
        using var srv = new FakeServer();
        using var c = new SkaidbConnection($"Seeds=127.0.0.1:{deadPort},127.0.0.1:{srv.Port};User=ada;Password=secret;Timeout=2");
        c.Open();
        Assert.True(c.IsUsable);
        Assert.Equal(1, srv.Connections);
        using var d = new SkaidbConnection($"Seeds=127.0.0.1:{deadPort};User=ada;Password=secret;Timeout=1");
        var e = Assert.Throws<SkaidbException>(() => d.Open());
        Assert.Matches(@"no reachable endpoint in 127\.0\.0\.1:\d+: ", e.Message);
        Assert.DoesNotContain("One or more errors", e.Message);          // AggregateException unwrapped
    }

    [Fact]
    public void A_connection_the_server_dropped_is_redialled_and_the_prepared_cache_reset()
    {
        int prepares = 0;
        bool dropNext = false;
        using var srv = new FakeServer((req, conn) =>
        {
            if (req.Op == 2) { prepares++; return new[] { F.Prepared(1, 1) }; }
            if (req.Op == 3)
            {
                if (dropNext)
                {
                    // Die mid-request: the client never gets a reply.
                    dropNext = false;
                    conn.Sock.Shutdown(SocketShutdown.Both);
                    conn.Sock.Close();
                    return Array.Empty<byte[]>();
                }
                return new[] { F.Rows(new[] { "v" }, new object?[] { req.Params[0] }) };
            }
            return null;
        });
        using var c = Connect(srv);
        Assert.Equal(7L, Scalar(c, "SELECT ?", 7));
        Assert.Equal(8L, Scalar(c, "SELECT ?", 8));
        Assert.Equal(1, prepares);
        // Closed while IDLE: nothing is in flight, so the next statement
        // re-dials before sending anything, re-prepares and runs.
        srv.CloseAll();
        Thread.Sleep(50);
        Assert.False(c.IsUsable);
        Assert.Equal(9L, Scalar(c, "SELECT ?", 9));
        Assert.True(c.IsUsable);
        Assert.Equal(2, prepares);
        Assert.Equal(2, srv.Connections);
        Assert.Equal(2, srv.Hellos.Count);
        // Closed MID-REQUEST: the in-flight statement fails (it may have
        // executed, so it is never retried) and the one after re-dials.
        dropNext = true;
        var e = Assert.Throws<SkaidbException>(() => Scalar(c, "SELECT ?", 10));
        Assert.Contains("connection closed by server", e.Message);
        Assert.False(c.IsUsable);
        Assert.Equal(11L, Scalar(c, "SELECT ?", 11));
        Assert.Equal(3, prepares);
        Assert.Equal(3, srv.Connections);
    }

    [Fact]
    public void Pool_reuses_idle_connections_discards_broken_ones_and_bounds_the_idle_set()
    {
        var s = new Scripted().Answer("SELECT 1", F.Rows(new[] { "v" }, new object?[] { 1L }));
        s.Script["SELECT drop"] = (req, conn) =>
        {
            conn.Sock.Shutdown(SocketShutdown.Both);
            conn.Sock.Close();
            return Array.Empty<byte[]>();
        };
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5", maxsize: 2);
        Assert.Equal(1L, pool.WithConnection(c => Scalar(c, "SELECT 1")));
        Assert.Equal(1L, pool.WithConnection(c => Scalar(c, "SELECT 1")));
        Assert.Equal(1, srv.Connections);                                  // reused
        var a = pool.Acquire(); var b = pool.Acquire(); var d = pool.Acquire();
        Assert.Equal(3, srv.Connections);
        pool.Release(a); pool.Release(b); pool.Release(d);               // the third exceeds maxsize and is disposed
        Assert.False(d.IsUsable);
        Assert.True(a.IsUsable);
        // A connection that broke while checked out is disposed on Release, not pooled.
        var x = pool.Acquire();
        Assert.Throws<SkaidbException>(() => Scalar(x, "SELECT drop"));
        Assert.False(x.IsUsable);
        pool.Release(x);
        Assert.False(x.IsOpen);
        // An idle connection the server closed is detected on Acquire and discarded.
        srv.CloseAll();
        Thread.Sleep(50);
        int before = srv.Connections;
        Assert.Equal(1L, pool.WithConnection(c => Scalar(c, "SELECT 1")));
        Assert.Equal(before + 1, srv.Connections);
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool("Host=x", 0));
        pool.Dispose();
        Assert.Throws<SkaidbException>(() => pool.Acquire());
    }

    [Fact]
    public void Subscribe_pages_the_stream_log_with_a_keyset_cursor_and_resumes_after_an_id()
    {
        var pages = new List<Request>();
        var s = new Scripted().Prepare("SELECT id, op, k, ts, doc FROM _stream_orders WHERE id > ? ORDER BY id LIMIT 500", 1);
        s.Answer("SELECT id, op, k, ts, doc FROM _stream_orders ORDER BY id LIMIT 500",
            F.Rows(new[] { "id", "op", "k", "ts", "doc" },
                new object?[] { "0001", "insert", 1L, 5L, new Dictionary<string, object?> { ["x"] = 1L } },
                new object?[] { "0002", "delete", 2L, 6L, null }));
        s.OnExecute = (stmt, req) =>
        {
            pages.Add(req);
            if ((string)req.Params[0]! == "0002") return new[] { F.Rows(new[] { "id", "op", "k", "ts", "doc" }, new object?[] { "0003", "update", 3L, 7L, null }) };
            return new[] { F.Rows(new[] { "id", "op", "k", "ts", "doc" }) };        // idle
        };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        var got = new List<SkaidbConnection.StreamEvent>();
        foreach (var ev in c.Subscribe("orders", pollMs: 10))
        {
            got.Add(ev);
            if (got.Count == 3) break;
        }
        Assert.Equal(new[] { "0001", "0002", "0003" }, got.Select(e => e.Id));
        Assert.Equal("insert", got[0].Op);
        Assert.Equal(1L, got[0].Key);
        Assert.Equal(1L, ((Dictionary<string, object?>)got[0].Doc!)["x"]);
        Assert.Equal("0002", pages[0].Params[0]);                          // resumed after the last id, as a string
        Assert.True(c.IsUsable);
        // Resuming from a saved id starts with the keyset query.
        foreach (var ev in c.Subscribe("orders", after: "0002", pollMs: 10)) { Assert.Equal("0003", ev.Id); break; }
        Assert.Equal("0002", pages.Last().Params[0]);
    }
}
