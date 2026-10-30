// The connection end to end against the in-process fake server: handshake,
// Hello, every request opcode, streaming (including the abandon/drain rule),
// batches, failover, reconnect, the pool and Subscribe.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Net.Sockets;
using System.Text;
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
        Assert.False(c.IsOpen);
        c.Open();                                   // Close is not final: it re-dials
        Assert.True(c.IsUsable);
        Assert.Equal(2, srv.Hellos.Count);
        c.Dispose();                                // Dispose is
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
            .Answer("SELECT 1 FROM no_such_table", F.Error("table \"no_such_table\" does not exist"))
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
            cmd.CommandText = "SELECT 1 FROM no_such_table";
            var e = Assert.Throws<SkaidbException>(() => cmd.ExecuteReader());
            Assert.Equal("table \"no_such_table\" does not exist", e.Message);
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
        Assert.Single(srv.Requests, q => q.Op == 2);                     // prepared once
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
        Assert.DoesNotContain(srv.Requests, q => q.Op == 1);              // no text retry
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
        // A ResultSets reply on the stream opcode is one whole frame: its sets come
        // back (see the CALL test below) and the same socket goes on serving.
        Assert.Equal(new[] { 1L }, c.Stream("CALL emits()").Select(r => (long)r[0]!).ToList());
        Assert.True(c.IsUsable);
        Assert.Equal(9L, Scalar(c, "SELECT id FROM t"));
        Assert.Equal(1, srv.Connections);
        // Per-stream consistency: the stream request carries it.
        Assert.Equal(2L, c.Stream("SELECT lvl", SkaidbConsistency.All).Single()[0]);
        Assert.Equal(2, srv.Requests.Last(q => q.Op == 5).Consistency);
        Assert.Equal(1L, c.Stream("SELECT lvl").Single()[0]);
        Assert.Equal(1, srv.Requests.Last(q => q.Op == 5).Consistency);
    }

    [Fact]
    public void A_streamed_CALL_that_EMITs_returns_its_result_sets_through_NextResult()
    {
        // One ResultSets frame, as ExecuteReader() receives it: [a: x, y] then [b: z].
        byte[] sets = F.ResultSets((new[] { "a" }, new object?[][] { new object?[] { "x" }, new object?[] { "y" } }),
                                   (new[] { "b" }, new object?[][] { new object?[] { "z" } }));
        var s = new Scripted()
            .Answer("CALL report(?)", sets)     // with parameters: OP_QUERY_STREAM_PARAMS
            .Answer("CALL report()", sets)      // without: OP_QUERY_STREAM
            .Answer("SELECT 1", F.Rows(new[] { "x" }, new object?[] { 1L }))
            .Answer("SELECT id FROM big", F.Header("id"), F.Chunk(new object?[] { 1L }), F.End());
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        var streams = new Func<IEnumerable<object?[]>>[]
        {
            () => c.Stream("CALL report(?)", new object?[] { 5L }),
            () => c.Stream("CALL report()"),
        };
        foreach (var stream in streams)
        {
            // The enumeration yields the first set, as a reader's Read() loop does ...
            Assert.Equal(new[] { "x", "y" }, stream().Select(r => (string)r[0]!).ToList());
            Assert.Equal(new[] { "a" }, c.StreamColumns);
            Assert.Null(c.StreamAffected);
            Assert.True(c.IsUsable);
            // ... and every set, in order, is on the reader ExecuteReader() would return.
            var r = c.StreamResultSets!;
            var walked = new List<string>();
            do
            {
                var cells = new StringBuilder(r.GetName(0)).Append(':');
                while (r.Read()) cells.Append(r.GetString(0));
                walked.Add(cells.ToString());
            } while (r.NextResult());
            Assert.Equal("a:xy|b:z", string.Join("|", walked));
        }
        Assert.Equal(new byte[] { 9, 5 }, srv.Requests.Where(q => q.Op != 8).Select(q => q.Op).ToArray());
        Assert.Equal(new object?[] { 5L }, srv.Requests.First(q => q.Op == 9).Params);
        // The connection is free while the rows are read, and the CALL's enumerator
        // finishing does not release a stream started meanwhile.
        using (var inner = c.Stream("SELECT id FROM big").GetEnumerator())
        {
            foreach (var row in c.Stream("CALL report()"))
            {
                if ((string)row[0]! != "x") continue;
                Assert.Equal(1L, Scalar(c, "SELECT 1"));
                Assert.True(inner.MoveNext());
            }
            Assert.False(c.IsUsable, "the inner stream still owns the connection");
            Assert.False(inner.MoveNext());
        }
        // Nothing was left on the socket: the same connection reads its own reply.
        Assert.True(c.IsUsable);
        Assert.Equal(1L, Scalar(c, "SELECT 1"));
        Assert.Empty(c.Stream("SELECT id FROM big").Skip(1));
        Assert.Null(c.StreamResultSets);
        Assert.Equal(1, srv.Connections);
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

    private static byte[] Le32(int v) => new[] { (byte)v, (byte)(v >> 8), (byte)(v >> 16), (byte)(v >> 24) };

    [Fact]
    public void Stream_with_parameters_sends_OP_QUERY_STREAM_PARAMS_carrying_the_OP_EXECUTE_parameter_vector()
    {
        const string sql = "SELECT id FROM t WHERE ts >= ? AND tag = ?";
        var s = new Scripted()
            .Answer(sql, F.Header("id"), F.Chunk(new object?[] { 7L }, new object?[] { 8L }), F.End())
            .Answer("DELETE FROM t WHERE id = ?", F.Mutation(1))
            .Prepare(sql, 2);
        s.OnExecute = (_, _) => new[] { F.Rows(new[] { "id" }, new object?[] { 7L }) };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);                                       // Consistency=Quorum (1)

        Assert.Equal(new[] { 7L, 8L }, c.Stream(sql, new object?[] { 5L, "a" }).Select(r => (long)r[0]!).ToList());
        Assert.Equal(new[] { "id" }, c.StreamColumns);
        Assert.True(c.IsUsable);

        // u8 9 | u8 consistency | u32 sql_len | sql | u16 nparams | nparams x (u32 len | value), little-endian.
        byte[] sqlBytes = Encoding.UTF8.GetBytes(sql);
        byte[] want = new byte[] { 9, 1 }
            .Concat(Le32(sqlBytes.Length)).Concat(sqlBytes)
            .Concat(new byte[] { 2, 0 })                                         // two parameters
            .Concat(Le32(9)).Concat(new byte[] { 2, 5, 0, 0, 0, 0, 0, 0, 0 })    // Int 5
            .Concat(Le32(6)).Concat(new byte[] { 5, 1, 0, 0, 0, (byte)'a' })     // String "a"
            .ToArray();
        var streamed = srv.Requests.Single(q => q.Op == 9);
        Assert.Equal(want, streamed.Raw);
        Assert.Equal(new object?[] { 5L, "a" }, streamed.Params);

        // Exactly the parameter vector OP_EXECUTE carries for the same values.
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = sql;
            cmd.Parameters.Add(5L);
            cmd.Parameters.Add("a");
            Assert.Equal(7L, cmd.ExecuteScalar());
        }
        byte[] execute = srv.Requests.Single(q => q.Op == 3).Raw;          // u8 3 | u8 consistency | u32 id | vector
        Assert.Equal(execute.AsSpan(6).ToArray(), streamed.Raw.AsSpan(6 + sqlBytes.Length).ToArray());

        // The reply is OP_QUERY_STREAM's: a mutation answers with its count.
        Assert.Empty(c.Stream("DELETE FROM t WHERE id = ?", new object?[] { 3L }).ToList());
        Assert.Empty(c.StreamColumns);
        Assert.Equal(1L, c.StreamAffected);
    }

    [Fact]
    public void Stream_without_parameters_sends_the_original_OP_QUERY_STREAM_byte_for_byte()
    {
        var s = new Scripted().Answer("SELECT 1", F.Header("v"), F.Chunk(new object?[] { 1L }), F.End());
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Single(c.Stream("SELECT 1"));
        Assert.Single(c.Stream("SELECT 1", Array.Empty<object?>()));
        Assert.Single(c.Stream("SELECT 1", new List<object?>()));
        Assert.Single(c.Stream("SELECT 1", []));
        Assert.Single(c.Stream("SELECT 1", (IReadOnlyList<object?>?)null));
        // u8 5 | u8 consistency | u32 sql_len | sql: what every server knows.
        byte[] want = new byte[] { 5, 1 }.Concat(Le32(8)).Concat(Encoding.UTF8.GetBytes("SELECT 1")).ToArray();
        var sent = srv.Requests.Where(q => q.Op != 8).ToList();            // all but the Hello
        Assert.Equal(5, sent.Count);
        Assert.All(sent, q => Assert.Equal(want, q.Raw));
    }

    [Fact]
    public void A_consistency_argument_keeps_meaning_the_consistency_and_the_1_2_signature_stays()
    {
        var s = new Scripted();
        IEnumerable<byte[]> Level(Request req, FakeConnection conn) =>
            new[] { F.Header("c"), F.Chunk(new object?[] { (long)req.Consistency }), F.End() };
        s.Script["SELECT lvl"] = Level;
        s.Script["SELECT lvl WHERE x = ?"] = Level;
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);                                       // Consistency=Quorum (1)
        static long Got(IEnumerable<object?[]> rows) => (long)rows.Single()[0]!;

        // Every call form of Stream(sql, consistency?) binds as it did: OP_QUERY_STREAM, the level in its byte.
        SkaidbConsistency? unset = null;
        Assert.Equal(0L, Got(c.Stream("SELECT lvl", SkaidbConsistency.One)));
        Assert.Equal(2L, Got(c.Stream("SELECT lvl", consistency: SkaidbConsistency.All)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl", unset)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl", consistency: null)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl", null)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl", default)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl")));
        Assert.All(srv.Requests.Where(q => q.Op != 8), q => Assert.Equal(5, q.Op));

        // With parameters it is still the trailing argument, never a parameter.
        Assert.Equal(0L, Got(c.Stream("SELECT lvl WHERE x = ?", new object?[] { 1L }, SkaidbConsistency.One)));
        Assert.Equal(2L, Got(c.Stream("SELECT lvl WHERE x = ?", [1L], consistency: SkaidbConsistency.All)));
        Assert.Equal(1L, Got(c.Stream("SELECT lvl WHERE x = ?", [1L])));
        var bound = srv.Requests.Where(q => q.Op == 9).ToList();
        Assert.Equal(3, bound.Count);
        Assert.All(bound, q => Assert.Equal(new object?[] { 1L }, q.Params));

        // Binary and method-group compatibility: 1.2's Stream(string, SkaidbConsistency?) is unchanged.
        var original = typeof(SkaidbConnection).GetMethod(nameof(SkaidbConnection.Stream),
            new[] { typeof(string), typeof(SkaidbConsistency?) });
        Assert.NotNull(original);
        Assert.Equal(typeof(IEnumerable<object[]>), original!.ReturnType);
        Func<string, SkaidbConsistency?, IEnumerable<object?[]>> group = c.Stream;
        Assert.Equal(original.MethodHandle, group.Method.MethodHandle);
        // And on what the compiler bound: a second argument that is a consistency, a literal null or
        // default included, picks that overload, not the parameter list.
        static RuntimeMethodHandle Bound(Expression<Func<SkaidbConnection, IEnumerable<object?[]>>> call) =>
            ((MethodCallExpression)call.Body).Method.MethodHandle;
        Assert.Equal(original.MethodHandle, Bound(x => x.Stream("q", SkaidbConsistency.One)));
        Assert.Equal(original.MethodHandle, Bound(x => x.Stream("q", null)));
        Assert.Equal(original.MethodHandle, Bound(x => x.Stream("q", default)));
        Assert.NotEqual(original.MethodHandle, Bound(x => x.Stream("q", new object?[] { 1L }, null)));
    }

    [Fact]
    public void An_old_server_refusing_parameterised_streaming_names_it_and_leaves_the_connection_usable()
    {
        // A server before 0.330 answers opcode 9 with one ordinary Error frame.
        const string unknown = "protocol error: malformed message: unknown opcode";
        var s = new Scripted()
            .Answer("SELECT id FROM t", F.Header("id"), F.Chunk(new object?[] { 1L }), F.End())
            .Answer("SELECT ancient", F.Error(unknown))                     // as a server without any streaming opcode
            .Answer("SELECT 42", F.Rows(new[] { "v" }, new object?[] { 42L }));
        using var srv = new FakeServer((req, conn) => req.Op == 9 ? new[] { F.Error(unknown) } : s.Handle(req, conn));
        using var c = Connect(srv);
        var e = Assert.Throws<SkaidbException>(() => c.Stream("SELECT id FROM t WHERE id > ?", new object?[] { 0L }).ToList());
        Assert.Equal($"server does not support parameterised streaming (skaidb >= 0.330): {unknown}", e.Message);
        Assert.True(c.IsUsable);                                           // one frame: nothing left to drain
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        Assert.Equal(new[] { 1L }, c.Stream("SELECT id FROM t").Select(r => (long)r[0]!).ToList());   // parameterless still streams
        Assert.Equal($"server does not support streaming: {unknown}",
            Assert.Throws<SkaidbException>(() => c.Stream("SELECT ancient").ToList()).Message);
        Assert.True(c.IsUsable);
        Assert.Equal(1, srv.Connections);
    }

    [Fact]
    public void A_stream_parameter_that_cannot_be_bound_fails_before_anything_is_sent()
    {
        var s = new Scripted().Answer("SELECT 42", F.Rows(new[] { "v" }, new object?[] { 42L }));
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Equal("cannot bind NaN/Infinity",
            Assert.Throws<SkaidbException>(() => c.Stream("SELECT v FROM t WHERE v > ?", new object?[] { double.NaN }).ToList()).Message);
        Assert.Equal("a statement takes at most 65535 parameters, got 65536",
            Assert.Throws<SkaidbException>(() => c.Stream("SELECT 1", new object?[65536]).ToList()).Message);
        Assert.True(c.IsUsable);                                           // never claimed
        Assert.DoesNotContain(srv.Requests, q => q.Op == 5 || q.Op == 9);
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
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
