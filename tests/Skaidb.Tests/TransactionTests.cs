// Transactions against the in-process fake server: the typed conflict error
// on every path that surfaces a server error, and the retrying helper —
// asserted by the statements the server received, in order.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Skaidb.Tests;

/// <summary>
/// A fake standalone server for transactions. Every statement that reaches
/// it — as text (OP_QUERY) or as the execution of a prepared statement
/// (OP_EXECUTE, logged by its text) — is appended to <see cref="Log"/> with
/// the socket it arrived on. <see cref="Answer"/> scripts a reply from the
/// statement and how many times that text has arrived so far (1 the first
/// time); null leaves the default: one row for a SELECT, one affected row
/// for a mutation, Ddl for everything else (BEGIN, COMMIT, ROLLBACK).
/// <see cref="Drop"/> closes the socket instead of answering;
/// <see cref="Hold"/> never answers and keeps the socket open. A stream
/// request (OP_QUERY_STREAM, with or without parameters) is logged the same
/// way and answered with <see cref="Stream"/>'s frames, by default a header,
/// two one-row chunks and the end. A batch (OP_EXECUTE_BATCH) is logged once,
/// by its statement's text, and answered with its row count.
/// </summary>
public sealed class Standalone
{
    private readonly object _gate = new();
    private readonly List<string> _log = new();
    private readonly List<FakeConnection> _sockets = new();
    private readonly Dictionary<string, int> _seen = new();
    private readonly Dictionary<uint, string> _ids = new();
    private uint _nextId = 1;

    public Func<string, int, byte[]?>? Answer;
    public Func<string, bool>? Drop;
    public Func<string, bool>? Hold;
    public Func<string, byte[][]?>? Stream;

    public string[] Log { get { lock (_gate) return _log.ToArray(); } }
    public FakeConnection[] Sockets { get { lock (_gate) return _sockets.ToArray(); } }

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        lock (_gate)
        {
            switch (req.Op)
            {
                case 1:
                    return Statement(req.Sql, conn);
                case 2:
                {
                    uint id = _nextId++;
                    _ids[id] = req.Sql;
                    return new[] { F.Prepared(id, (ushort)req.Sql.Count(ch => ch == '?')) };
                }
                case 3:
                    return Statement(_ids[req.Id], conn);
                case 5:
                case 9:
                    return Statement(req.Sql, conn, streamed: true);
                case 7:
                    _log.Add(_ids[req.Id]);
                    _sockets.Add(conn);
                    return new[] { F.Mutation((ulong)req.Rows.Count) };
                default:
                    return null;   // Hello -> Ddl
            }
        }
    }

    private IEnumerable<byte[]> Statement(string sql, FakeConnection conn, bool streamed = false)
    {
        _log.Add(sql);
        _sockets.Add(conn);
        int nth = _seen[sql] = _seen.GetValueOrDefault(sql) + 1;
        if (Drop?.Invoke(sql) == true)
        {
            conn.Sock.Shutdown(SocketShutdown.Both);
            conn.Sock.Close();
            return Array.Empty<byte[]>();
        }
        if (Hold?.Invoke(sql) == true) return Array.Empty<byte[]>();
        if (streamed)
        {
            return Stream?.Invoke(sql) ?? new[]
            {
                F.Header("v"), F.Chunk(new object?[] { 1L }), F.Chunk(new object?[] { 2L }), F.End(),
            };
        }
        if (Answer?.Invoke(sql, nth) is byte[] scripted) return new[] { scripted };
        if (sql.StartsWith("SELECT", StringComparison.Ordinal)) return new[] { F.Rows(new[] { "v" }, new object?[] { 7L }) };
        if (sql.StartsWith("UPDATE", StringComparison.Ordinal)) return new[] { F.Mutation(1) };
        return new[] { F.Ddl() };
    }
}

public class TransactionTests
{
    /// <summary>The server's text today; only its prefix is contractual.</summary>
    private const string Conflict =
        "transaction conflict: a row of table accounts was changed by another writer after the "
        + "transaction began — nothing was committed; run the transaction again";

    private const string Update = "UPDATE accounts SET balance = balance - 100 WHERE id = 1";

    private static SkaidbConnection Connect(FakeServer srv)
    {
        var c = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5");
        c.Open();
        return c;
    }

    private static int Exec(SkaidbConnection c, string sql, params object?[] p)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in p) cmd.Parameters.Add(v);
        return cmd.ExecuteNonQuery();
    }

    private static object? Scalar(SkaidbConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static byte[]? ConflictIf(bool when) => when ? F.Error(Conflict) : null;

    // ---- the typed error ---------------------------------------------------------

    [Fact]
    public void A_transaction_conflict_is_typed_on_every_path_stays_a_SkaidbException_and_keeps_the_connection()
    {
        const string prepared = "UPDATE t SET v = ? WHERE id = 1";
        const string refusedAtPrepare = "UPDATE t SET w = ? WHERE id = 1";
        var s = new Scripted()
            .Answer("COMMIT", F.Error(Conflict))
            .Answer("SELECT early", F.Error(Conflict))
            .Answer("SELECT late", F.Header("id"), F.Chunk(new object?[] { 1L }), F.Error(Conflict))
            .Answer("SELECT 42", F.Rows(new[] { "v" }, new object?[] { 42L }))
            .Answer("SELECT wrapped", F.Error("row 0: " + Conflict))
            .Answer("SELECT short", F.Error("transaction conflict: whatever the server says next"))
            .Prepare(prepared, 1);
        s.OnExecute = (_, _) => new[] { F.Error(Conflict) };
        s.OnBatch = (_, _) => new[] { F.Error(Conflict) };
        using var srv = new FakeServer((req, conn) =>
            req.Op == 2 && req.Sql == refusedAtPrepare ? new[] { F.Error(Conflict) } : s.Handle(req, conn));
        using var c = Connect(srv);

        // The statement a conflict really answers: COMMIT, as text.
        var e = Assert.Throws<SkaidbTransactionConflictException>(() => Exec(c, "COMMIT"));
        Assert.Equal(Conflict, e.Message);                                       // the server's message, unchanged
        Assert.IsAssignableFrom<SkaidbException>(e);
        Assert.IsAssignableFrom<DbException>(e);
        Assert.Null(e.InnerException);
        // Code written against the statement error keeps catching it.
        string? caught = null;
        try { Exec(c, "COMMIT"); }
        catch (SkaidbException old) { caught = old.Message; }
        Assert.Equal(Conflict, caught);
        // A statement error, not a connection failure: same socket, next statement runs.
        Assert.True(c.IsUsable);
        Assert.Equal(42L, Scalar(c, "SELECT 42"));

        // Every other path that surfaces a server error maps it the same way.
        Assert.Throws<SkaidbTransactionConflictException>(() => Scalar(c, "COMMIT"));                    // ExecuteScalar
        Assert.Throws<SkaidbTransactionConflictException>(() =>
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "COMMIT";
            using var r = cmd.ExecuteReader();
        });
        Assert.Throws<SkaidbTransactionConflictException>(() => Exec(c, prepared, 5L));                  // OP_EXECUTE
        Assert.Throws<SkaidbTransactionConflictException>(() => Exec(c, refusedAtPrepare, 5L));          // OP_PREPARE
        Assert.Throws<SkaidbTransactionConflictException>(() =>                                          // OP_EXECUTE_BATCH
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = prepared;
            cmd.ExecuteBatch(new List<IReadOnlyList<object?>> { new object?[] { 1L }, new object?[] { 2L } });
        });
        Assert.Throws<SkaidbTransactionConflictException>(() =>                                          // DbBatch
        {
            using var batch = c.CreateBatch();
            batch.BatchCommands.Add("COMMIT");
            batch.ExecuteNonQuery();
        });
        Assert.Throws<SkaidbTransactionConflictException>(() => c.Stream("SELECT early").ToList());      // a stream's first frame
        var rows = new List<long>();
        Assert.Throws<SkaidbTransactionConflictException>(() =>                                          // a stream, after rows
        {
            foreach (var row in c.Stream("SELECT late")) rows.Add((long)row[0]!);
        });
        Assert.Equal(new[] { 1L }, rows);
        Assert.True(c.IsUsable);
        Assert.Equal(42L, Scalar(c, "SELECT 42"));
        Assert.Equal(1, srv.Connections);                                        // never re-dialed

        // The prefix decides, nothing else: any text behind it is a conflict,
        // and the same words further into a message are not.
        Assert.Throws<SkaidbTransactionConflictException>(() => Scalar(c, "SELECT short"));
        Assert.Equal("row 0: " + Conflict, Assert.Throws<SkaidbException>(() => Scalar(c, "SELECT wrapped")).Message);
    }

    [Fact]
    public void A_conflict_does_not_count_against_a_pool_member_or_cost_the_pool_its_connection()
    {
        var s = new Standalone { Answer = (sql, _) => ConflictIf(sql == "COMMIT") };
        using var a = new FakeServer(s.Handle);
        using var b = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool(
            $"Host=127.0.0.1:{a.Port},127.0.0.1:{b.Port};User=ada;Password=secret;Timeout=5",
            new SkaidbPoolOptions { EjectErrors = 1 });                          // ONE counted failure would eject the member
        for (int i = 0; i < 4; i++)
        {
            Assert.Throws<SkaidbTransactionConflictException>(() => pool.WithConnection(c =>
            {
                Exec(c, "BEGIN");
                Exec(c, Update);
                return Exec(c, "COMMIT");
            }));
        }
        var members = pool.Members();
        Assert.All(members, m => Assert.Equal(SkaidbMemberState.Ready, m.State));
        Assert.Equal(1, members.Sum(m => m.Idle));                               // released into the pool every time ...
        Assert.Equal(1, a.Connections + b.Connections);                          // ... and handed out again: one socket in all
        Assert.Equal(12, s.Log.Length);
    }

    // ---- the helper ----------------------------------------------------------------

    [Fact]
    public void A_transaction_sends_BEGIN_the_body_statements_and_COMMIT_once_and_returns_the_body_value()
    {
        const string bound = "UPDATE accounts SET balance = balance + ? WHERE id = ?";
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        int runs = 0;
        long got = c.Transaction(conn =>
        {
            runs++;
            Assert.Same(c, conn);                                                // the body is handed the connection itself
            Exec(conn, Update);
            Exec(conn, bound, 100L, 2L);                                         // a prepared statement is part of it too
            return (long)Scalar(conn, "SELECT balance FROM accounts WHERE id = 2")!;
        });
        Assert.Equal(7L, got);
        Assert.Equal(1, runs);
        Assert.Equal(new[] { "BEGIN", Update, bound, "SELECT balance FROM accounts WHERE id = 2", "COMMIT" }, s.Log);
        Assert.Single(s.Sockets.Distinct());
        Assert.Equal(1, srv.Connections);
        Assert.True(c.IsUsable);

        // A body that returns nothing, and one whose value is a reference.
        c.Transaction(conn => { Exec(conn, Update); });
        string? nothing = c.Transaction(conn => { Exec(conn, Update); return (string?)null; });
        Assert.Null(nothing);
        Assert.Equal(new[] { "BEGIN", Update, "COMMIT", "BEGIN", Update, "COMMIT" }, s.Log.Skip(5));
    }

    [Fact]
    public void A_conflict_on_COMMIT_runs_the_whole_transaction_again_and_the_last_run_s_value_is_returned()
    {
        var s = new Standalone { Answer = (sql, nth) => ConflictIf(sql == "COMMIT" && nth <= 2) };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        int runs = 0;
        string got = c.Transaction(conn =>
        {
            runs++;
            Exec(conn, Update);
            return "run " + runs;
        }, attempts: 3);
        Assert.Equal(3, runs);
        Assert.Equal("run 3", got);
        Assert.Equal(new[]
        {
            "BEGIN", Update, "COMMIT",          // refused
            "BEGIN", Update, "COMMIT",          // refused
            "BEGIN", Update, "COMMIT",          // committed
        }, s.Log);                               // and never a ROLLBACK: a refused COMMIT ended the transaction
        Assert.Single(s.Sockets.Distinct());
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void A_conflict_on_every_COMMIT_is_thrown_after_exactly_the_given_number_of_runs()
    {
        var s = new Standalone { Answer = (sql, _) => ConflictIf(sql == "COMMIT") };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        int runs = 0;
        var e = Assert.Throws<SkaidbTransactionConflictException>(() => c.Transaction(conn =>
        {
            runs++;
            return Exec(conn, Update);
        }, attempts: 2));
        Assert.Equal(Conflict, e.Message);
        Assert.Equal(2, runs);
        Assert.Equal(new[] { "BEGIN", Update, "COMMIT", "BEGIN", Update, "COMMIT" }, s.Log);
        // One attempt means no retry at all; the default is three.
        Assert.Throws<SkaidbTransactionConflictException>(() => c.Transaction(conn => { runs++; }, attempts: 1));
        Assert.Equal(3, runs);
        Assert.Throws<SkaidbTransactionConflictException>(() => c.Transaction(conn => { runs++; }));
        Assert.Equal(6, runs);
        Assert.True(c.IsUsable);
        Assert.Equal(1, srv.Connections);
    }

    [Fact]
    public void A_body_error_sends_ROLLBACK_and_is_rethrown_unchanged_whatever_ROLLBACK_answers()
    {
        // ROLLBACK answers success, or an Error (what a server before 0.347
        // says when nothing is open): the body's exception wins either way.
        foreach (byte[]? rollback in new[] { null, F.Error("no transaction in progress") })
        {
            var s = new Standalone { Answer = (sql, _) => sql == "ROLLBACK" ? rollback : null };
            using var srv = new FakeServer(s.Handle);
            using var c = Connect(srv);
            var mine = new InvalidOperationException("mine");
            int runs = 0;
            var thrown = Assert.Throws<InvalidOperationException>(() => c.Transaction<long>(conn =>
            {
                runs++;
                Exec(conn, Update);
                throw mine;
            }));
            Assert.Same(mine, thrown);
            Assert.Equal(1, runs);
            Assert.Equal(new[] { "BEGIN", Update, "ROLLBACK" }, s.Log);          // and no COMMIT
            Assert.True(c.IsUsable);
            Assert.Equal(7L, Scalar(c, "SELECT 1"));
        }
    }

    [Fact]
    public void A_failing_statement_in_the_body_rolls_back_and_a_conflict_out_of_the_body_is_not_retried()
    {
        const string missing = "UPDATE no_such_table SET v = 1";
        var s = new Standalone
        {
            Answer = (sql, _) => sql == missing ? F.Error("table \"no_such_table\" does not exist")
                : sql == Update ? F.Error(Conflict)
                : sql == "ROLLBACK" ? F.Error("no transaction in progress")
                : null,
        };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn => { Exec(conn, missing); }));
        Assert.Equal("table \"no_such_table\" does not exist", e.Message);       // not the ROLLBACK's error
        Assert.Equal(new[] { "BEGIN", missing, "ROLLBACK" }, s.Log);

        // Only a refused COMMIT is retried: the same error out of the body is the body's error.
        int runs = 0;
        Assert.Throws<SkaidbTransactionConflictException>(() => c.Transaction(conn =>
        {
            runs++;
            return Exec(conn, Update);
        }, attempts: 3));
        Assert.Equal(1, runs);
        Assert.Equal(new[] { "BEGIN", Update, "ROLLBACK" }, s.Log.Skip(3));
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void A_refused_BEGIN_is_thrown_the_body_never_runs_and_nothing_else_is_sent()
    {
        var s = new Standalone { Answer = (sql, _) => sql == "BEGIN" ? F.Error("BEGIN refused") : null };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        bool ran = false;
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            ran = true;
            Exec(conn, Update);
        }));
        Assert.Equal("BEGIN refused", e.Message);
        Assert.False(ran);
        Assert.Equal(new[] { "BEGIN" }, s.Log);                                  // no statement, no COMMIT, no ROLLBACK
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void A_COMMIT_error_that_is_not_a_conflict_is_thrown_without_a_retry_or_a_ROLLBACK()
    {
        var s = new Standalone { Answer = (sql, _) => sql == "COMMIT" ? F.Error("commit failed: quorum lost") : null };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        int runs = 0;
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>      // exactly SkaidbException
        {
            runs++;
            return Exec(conn, Update);
        }, attempts: 5));
        Assert.Equal("commit failed: quorum lost", e.Message);
        Assert.Equal(1, runs);
        Assert.Equal(new[] { "BEGIN", Update, "COMMIT" }, s.Log);
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void A_transport_failure_is_the_usual_connection_error_and_is_not_retried()
    {
        var s = new Standalone { Drop = sql => sql == "COMMIT" };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        int runs = 0;
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            runs++;
            return Exec(conn, Update);
        }));
        Assert.Contains("connection closed by server", e.Message);
        Assert.Equal(1, runs);
        Assert.Equal(new[] { "BEGIN", Update, "COMMIT" }, s.Log);
        Assert.False(c.IsUsable);                                                // broken, as after any transport error
    }

    [Fact]
    public void A_connection_lost_between_statements_ends_the_transaction_instead_of_re_dialing_into_autocommit()
    {
        const string second = "UPDATE accounts SET balance = balance + 100 WHERE id = 2";
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);

        // The server goes away while the connection sits idle between two
        // statements of the body. Outside the helper the next statement
        // would re-dial and run; here it would run without its BEGIN.
        int runs = 0;
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            runs++;
            Exec(conn, Update);
            srv.CloseAll(seenBy: conn);
            Exec(conn, second);
        }));
        Assert.Contains("connection lost inside a transaction", e.Message);
        Assert.Equal(1, runs);
        Assert.Equal(new[] { "BEGIN", Update }, s.Log);                          // nothing since: no statement, no ROLLBACK
        Assert.Equal(1, srv.Connections);                                        // and no new connection to send one on
        Assert.False(c.IsUsable);
        // The helper has returned: the connection re-dials as it always does.
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        Assert.Equal(2, srv.Connections);

        // The same loss just before the COMMIT: it is not sent to a
        // connection that has no transaction to commit.
        e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            runs++;
            Exec(conn, Update);
            srv.CloseAll(seenBy: conn);
        }));
        Assert.Contains("connection lost inside a transaction", e.Message);
        Assert.Equal(2, runs);
        Assert.Equal(new[] { "SELECT 1", "BEGIN", Update }, s.Log.Skip(2));
        Assert.Equal(2, srv.Connections);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));                                 // usable again afterwards
        Assert.Equal(3, srv.Connections);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Attempts_below_one_is_an_argument_error_and_nothing_is_sent(int attempts)
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        bool ran = false;
        var e = Assert.Throws<ArgumentOutOfRangeException>(() => c.Transaction(conn => { ran = true; }, attempts));
        Assert.IsAssignableFrom<ArgumentException>(e);
        Assert.Equal("attempts", e.ParamName);
        Assert.Throws<ArgumentOutOfRangeException>(() => c.Transaction(conn => { ran = true; return 1; }, attempts));
        Assert.Throws<ArgumentNullException>(() => c.Transaction((Action<SkaidbConnection>)null!));
        Assert.Throws<ArgumentNullException>(() => c.Transaction((Func<SkaidbConnection, int>)null!));
        Assert.Throws<ArgumentNullException>(() => c.Transaction(conn => { ran = true; }, begin: null!));
        Assert.False(ran);
        Assert.Empty(s.Log);
        Assert.DoesNotContain(srv.Requests, q => q.Op != 8);                     // the Hello of Open(), nothing since

        // A pool refuses before it checks a connection out (which would dial one).
        using var other = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool($"Host=127.0.0.1;Port={other.Port};User=ada;Password=secret;Timeout=5");
        Assert.Throws<ArgumentOutOfRangeException>(() => pool.Transaction(conn => { ran = true; }, attempts));
        Assert.Throws<ArgumentOutOfRangeException>(() => pool.Transaction(conn => { ran = true; return 1; }, attempts));
        Assert.False(ran);
        Assert.Equal(0, other.Connections);
    }

    [Fact]
    public void A_body_that_returns_a_task_is_refused_before_anything_is_sent()
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        bool ran = false;
        // What an async lambda binds to: Transaction<Task>. Committing when it
        // returned its task would commit before, or despite, what it awaits.
        var e = Assert.Throws<ArgumentException>(() =>
        {
            _ = c.Transaction(async conn => { ran = true; await Task.Yield(); });
        });
        Assert.Equal("body", e.ParamName);
        Assert.Throws<ArgumentException>(() => { _ = c.Transaction(conn => { ran = true; return Task.FromResult(1); }); });
        Assert.Throws<ArgumentException>(() => { _ = c.Transaction(conn => { ran = true; return new ValueTask<int>(1); }); });
        Assert.Throws<ArgumentException>(() => { _ = c.Transaction(conn => { ran = true; return ValueTask.CompletedTask; }); });
        Assert.False(ran);
        Assert.Empty(s.Log);
    }

    [Fact]
    public void A_custom_begin_text_is_sent_verbatim_on_every_attempt()
    {
        const string begin = "BEGIN ATOMIC PARTITION (tenant_id = 't1')";
        var s = new Standalone { Answer = (sql, nth) => ConflictIf(sql == "COMMIT" && nth == 1) };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        c.Transaction(conn => { Exec(conn, Update); }, begin: begin);
        Assert.Equal(new[] { begin, Update, "COMMIT", begin, Update, "COMMIT" }, s.Log);
        Assert.Equal(9L, c.Transaction(conn => 9L, 3, "BEGIN ATOMIC"));          // positionally, after attempts
        Assert.Equal(new[] { "BEGIN ATOMIC", "COMMIT" }, s.Log.Skip(6));
        Assert.DoesNotContain("BEGIN", s.Log);                                   // the default text is never added
    }

    // ---- on a pool -------------------------------------------------------------------

    [Fact]
    public void A_pool_transaction_holds_one_connection_for_every_attempt_and_returns_it()
    {
        var s = new Standalone { Answer = (sql, nth) => ConflictIf(sql == "COMMIT" && nth <= 2) };
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5", maxsize: 4);
        // Two idle connections, so a helper that went back to the pool
        // between statements or attempts would have another one to land on.
        var x = pool.Acquire();
        var y = pool.Acquire();
        pool.Release(x);
        pool.Release(y);

        var handed = new List<SkaidbConnection>();
        string got = pool.Transaction(c =>
        {
            handed.Add(c);
            Exec(c, Update);
            return "run " + handed.Count;
        });
        Assert.Equal("run 3", got);
        Assert.Single(handed.Distinct());                                        // the same connection each run
        Assert.Equal(9, s.Log.Length);                                           // 3 x (BEGIN, UPDATE, COMMIT)
        Assert.Single(s.Sockets.Distinct());                                     // all nine on one socket
        Assert.Equal(2, srv.Connections);
        Assert.Equal(2, pool.Members().Sum(m => m.Idle));                        // returned afterwards
        Assert.Equal(0, pool.Members().Sum(m => m.InUse));

        // A body error rolls back on that connection and still returns it.
        Assert.Throws<InvalidOperationException>(() => pool.Transaction(c =>
        {
            Exec(c, Update);
            throw new InvalidOperationException("mine");
        }));
        Assert.Equal(new[] { "BEGIN", Update, "ROLLBACK" }, s.Log.Skip(9));
        Assert.Single(s.Sockets.Skip(9).Distinct());
        Assert.Equal(2, pool.Members().Sum(m => m.Idle));
        Assert.Equal(2, srv.Connections);
    }

    // ---- the transaction's session: a lost connection, a restart, an open stream, a timeout ----

    private static string Cs(FakeServer srv, string timeout = "5") =>
        $"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout={timeout}";

    private static void WaitFor(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            Thread.Sleep(20);
        }
    }

    /// <summary>Every way a body can run a statement on its connection, each a statement of its own.</summary>
    private static (string Way, Action Run)[] Ways(SkaidbConnection c)
    {
        const string text = "UPDATE audit SET n = n + 1 WHERE id = 1";
        const string bound = "UPDATE audit SET n = n + ? WHERE id = ?";
        var rows = new List<IReadOnlyList<object?>> { new object?[] { 1L, 1L }, new object?[] { 1L, 2L } };
        return new (string, Action)[]
        {
            ("ExecuteNonQuery, text", () => Exec(c, text)),
            ("ExecuteNonQuery, prepared", () => Exec(c, bound, 1L, 1L)),
            ("ExecuteScalar", () => Scalar(c, "SELECT n FROM audit WHERE id = 1")),
            ("ExecuteReader", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = text;
                using var r = cmd.ExecuteReader();
            }),
            ("ExecuteNonQueryAsync", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = text;
                cmd.ExecuteNonQueryAsync().GetAwaiter().GetResult();
            }),
            ("Prepare", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = bound;
                cmd.Parameters.Add(1L);
                cmd.Parameters.Add(1L);
                cmd.Prepare();
            }),
            ("ExecuteBatch", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = bound;
                cmd.ExecuteBatch(rows);
            }),
            ("DbBatch, one statement", () =>
            {
                using var batch = c.CreateBatch();
                batch.BatchCommands.Add(text);
                batch.ExecuteNonQuery();
            }),
            ("DbBatch, a merged run", () =>
            {
                using var batch = c.CreateBatch();
                foreach (var row in rows)
                {
                    var one = batch.BatchCommands.Add(bound);
                    foreach (var v in row) one.Parameters.Add(v);
                }
                batch.ExecuteNonQuery();
            }),
            ("Stream", () => c.Stream("SELECT n FROM audit").ToList()),
            ("Stream, parameters", () => c.Stream("SELECT n FROM audit WHERE id = ?", new object?[] { 1L }).ToList()),
            ("ChangeDatabase", () => c.ChangeDatabase("other")),
        };
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void After_a_connection_loss_the_body_swallowed_no_later_statement_and_no_COMMIT_reaches_a_new_connection(
        bool underAStatement, bool pooled)
    {
        const string second = "UPDATE accounts SET balance = balance + 100 WHERE id = 2";
        // The server goes away under statement 2, or while the connection
        // sits idle just before it.
        var s = new Standalone { Drop = sql => underAStatement && sql == second };
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv));
        using var single = new SkaidbConnection(Cs(srv));
        if (!pooled) single.Open();

        SkaidbException? two = null;
        var later = new List<(string Way, string Message)>();
        Func<SkaidbConnection, string> body = conn =>
        {
            Exec(conn, Update);
            if (!underAStatement) srv.CloseAll(seenBy: conn);
            try { Exec(conn, second); }
            catch (SkaidbException e) { two = e; }
            // The body swallowed that and carries on. Whichever way it runs
            // its next statement, a re-dial would apply it in autocommit.
            foreach (var (way, run) in Ways(conn))
                later.Add((way, Assert.Throws<SkaidbException>(run).Message));
            return "the body returned";
        };
        var lost = Assert.Throws<SkaidbException>(() => pooled ? pool.Transaction(body) : single.Transaction(body));

        // Statement 2 fails as it does on any lost connection ...
        Assert.Contains(underAStatement ? "connection closed by server" : "connection lost inside a transaction", two!.Message);
        // ... every statement after it is refused ...
        Assert.Equal(Ways(single).Length, later.Count);
        Assert.All(later, l => Assert.Contains("connection lost inside a transaction", l.Message));
        // ... and the helper reports the loss, not the body's value: its
        // COMMIT has no transaction to commit on another connection.
        Assert.Contains("connection lost inside a transaction", lost.Message);
        Assert.Equal(underAStatement ? new[] { "BEGIN", Update, second } : new[] { "BEGIN", Update }, s.Log);
        Assert.Equal(1, srv.Connections);                                        // nothing was dialed to send them on
        Assert.DoesNotContain(srv.Requests, q => q.Op != 8 && q.Op != 1);        // not even a PREPARE went out

        // The helper has returned: the connection is what it is after any
        // connection loss, and the next statement gets a new one.
        if (pooled)
        {
            Assert.Equal(0, pool.Members().Sum(m => m.Idle + m.InUse));          // the pool dropped it
            Assert.Equal(7L, pool.WithConnection(c => Scalar(c, "SELECT 1")));
        }
        else
        {
            Assert.False(single.IsUsable);
            Assert.Equal(7L, Scalar(single, "SELECT 1"));
            Assert.True(single.IsUsable);
        }
        Assert.Equal(2, srv.Connections);
        Assert.Equal("SELECT 1", s.Log[^1]);
        Assert.NotSame(s.Sockets[0], s.Sockets[^1]);
    }

    [Fact]
    public void A_connection_the_body_closed_cannot_be_opened_again_to_take_the_rest_of_the_transaction()
    {
        const string second = "UPDATE accounts SET balance = balance + 100 WHERE id = 2";
        const string third = "UPDATE audit SET n = n + 1 WHERE id = 1";
        var s = new Standalone { Drop = sql => sql == second };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        SkaidbException? reopen = null, three = null;
        var lost = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            Exec(conn, Update);
            try { Exec(conn, second); }
            catch (SkaidbException)
            {
                // "Reconnect and carry on": what Open() would dial never saw
                // the BEGIN, so the rest of the body would autocommit on it.
                conn.Close();
                reopen = Assert.Throws<SkaidbException>(() => conn.Open());
            }
            try { Exec(conn, third); }
            catch (SkaidbException e) { three = e; }
            return "the body returned";
        }));
        Assert.Contains("connection lost inside a transaction", reopen!.Message);
        Assert.Equal("connection is not open", three!.Message);
        Assert.Equal("connection is not open", lost.Message);                    // the helper's COMMIT: not the body's value
        Assert.Equal(new[] { "BEGIN", Update, second }, s.Log);                  // neither the statement nor a COMMIT
        Assert.Equal(1, srv.Connections);                                        // and nothing was dialed for them
        // The transaction is over: the connection is closed, as the body
        // left it, and opens like any other.
        Assert.False(c.IsOpen);
        c.Open();
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        Assert.Equal(2, srv.Connections);
    }

    [Fact]
    public void The_opening_statement_re_dials_a_connection_the_server_closed_while_it_sat_idle()
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        // The server restarts while the client sits idle. Any statement
        // re-dials before it is sent, and so does the helper's BEGIN (nothing
        // is open yet): the transaction runs, whole, on the new connection.
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, c.Transaction(conn => Exec(conn, Update)));
        Assert.Equal(new[] { "SELECT 1", "BEGIN", Update, "COMMIT" }, s.Log);
        Assert.Equal(2, srv.Connections);
        Assert.NotSame(s.Sockets[0], s.Sockets[1]);
        Assert.Single(s.Sockets.Skip(1).Distinct());
        Assert.True(c.IsUsable);

        // A pool finds its idle connection dead at the checkout and dials.
        using var pool = new SkaidbConnectionPool(Cs(srv));
        SkaidbConnection? idle = null;
        Assert.Equal(7L, pool.WithConnection(p => Scalar(idle = p, "SELECT 1")));
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
        srv.CloseAll(seenBy: idle!);
        Assert.Equal(1, pool.Transaction(conn => Exec(conn, Update)));
        Assert.Equal(new[] { "SELECT 1", "BEGIN", Update, "COMMIT" }, s.Log.Skip(4));
        Assert.Equal(4, srv.Connections);
        Assert.Single(s.Sockets.Skip(5).Distinct());
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void A_stream_the_body_left_open_costs_the_connection_so_nothing_later_joins_the_transaction(
        bool bodyThrows, bool pooled)
    {
        const string big = "SELECT id FROM big";
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv));
        using var single = new SkaidbConnection(Cs(srv));
        if (!pooled) single.Open();

        var mine = new InvalidOperationException("mine");
        IEnumerator<object?[]>? left = null;
        Func<SkaidbConnection, long> body = conn =>
        {
            Exec(conn, Update);
            // Started, and neither run out nor disposed: the stream holds the
            // connection, so no COMMIT and no ROLLBACK can be sent on it.
            left = conn.Stream(big).GetEnumerator();
            Assert.True(left.MoveNext());
            if (bodyThrows) throw mine;
            return (long)left.Current[0]!;
        };
        var thrown = Assert.ThrowsAny<Exception>(() => pooled ? pool.Transaction(body) : single.Transaction(body));
        if (bodyThrows) Assert.Same(mine, thrown);
        else Assert.Contains("busy streaming", Assert.IsType<SkaidbException>(thrown).Message);
        Assert.Equal(new[] { "BEGIN", Update, big }, s.Log);

        // The transaction is still open on the server and only closing its
        // socket ends it: that happens before the helper returns, not when
        // the caller gets round to the connection again.
        WaitFor(() => srv.Live == 0, "the socket with the open transaction on it is still open");
        Assert.Equal(1, srv.Connections);
        if (pooled)
        {
            Assert.Equal(0, pool.Members().Sum(m => m.Idle + m.InUse));
            left!.Dispose();
            Assert.Equal(1, pool.WithConnection(c => Exec(c, Update)));
        }
        else
        {
            Assert.False(single.IsUsable);
            // Letting go of the stream frees the connection object, and its
            // next statement runs on a new session, in autocommit: not
            // inside the transaction nobody ended.
            left!.Dispose();
            Assert.Equal(1, Exec(single, Update));
            Assert.True(single.IsUsable);
        }
        Assert.Equal(2, srv.Connections);
        Assert.Equal(new[] { "BEGIN", Update, big, Update }, s.Log);
        Assert.NotSame(s.Sockets[0], s.Sockets[^1]);
    }

    [Fact]
    public void A_stream_the_body_closed_itself_leaves_the_transaction_to_commit()
    {
        const string big = "SELECT id FROM big";
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        long first = c.Transaction(conn =>
        {
            Exec(conn, Update);
            foreach (var row in conn.Stream(big)) return (long)row[0]!;         // abandoned after one row, and disposed
            return -1;
        });
        Assert.Equal(1L, first);
        Assert.Equal(new[] { "BEGIN", Update, big, "COMMIT" }, s.Log);
        Assert.Single(s.Sockets.Distinct());
        Assert.True(c.IsUsable);
        Assert.Equal(1, srv.Live);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_read_timeout_inside_a_transaction_closes_the_socket_at_once(bool swallowed)
    {
        const string slow = "UPDATE accounts SET balance = 0 WHERE id = 9";
        var s = new Standalone { Hold = sql => sql == slow };                    // never answered, the socket stays open
        using var srv = new FakeServer(s.Handle);
        using var c = new SkaidbConnection(Cs(srv, timeout: "1"));
        c.Open();
        SkaidbException? timedOut = null;
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            Exec(conn, Update);
            try { Exec(conn, slow); }
            catch (SkaidbException x) when (swallowed) { timedOut = x; }
        }));
        Assert.Contains(swallowed ? "connection lost inside a transaction" : "read failed", e.Message);
        if (swallowed) Assert.Contains("read failed", timedOut!.Message);
        Assert.Equal(new[] { "BEGIN", Update, slow }, s.Log);                    // no ROLLBACK, no COMMIT: there is nowhere to send one
        // The server is alive and still holds the session, its transaction
        // and the rows it claimed. Nothing but closing the socket releases
        // them, and the caller may not touch this connection again for hours.
        WaitFor(() => srv.Live == 0, "the timed-out socket is still open, and the server still has its transaction");
        Assert.Equal(1, srv.Connections);
        Assert.False(c.IsUsable);
        Assert.True(c.IsOpen);                                                   // broken, not closed: the next statement re-dials
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        Assert.Equal(2, srv.Connections);
        Assert.True(c.IsUsable);
    }

    [Fact]
    public void A_stream_abandoned_past_the_drain_budget_inside_a_transaction_ends_it_and_closes_the_socket()
    {
        const string big = "SELECT id FROM big";
        var frames = new List<byte[]> { F.Header("id") };
        for (int i = 0; i < 100; i++) frames.Add(F.Chunk(new object?[] { (long)i }));   // > 64-frame drain budget
        frames.Add(F.End());
        var s = new Standalone { Stream = _ => frames.ToArray() };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        var e = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            Exec(conn, Update);
            foreach (var _ in conn.Stream(big)) break;                           // gives the connection up: too much to drain
        }));
        Assert.Contains("connection lost inside a transaction", e.Message);
        Assert.Equal(new[] { "BEGIN", Update, big }, s.Log);
        WaitFor(() => srv.Live == 0, "the socket given up on is still open, and the server still has its transaction");
        Assert.Equal(1, srv.Connections);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        Assert.Equal(2, srv.Connections);
    }
}
