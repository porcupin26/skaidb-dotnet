// A transaction the application writes itself — BEGIN, its statements,
// COMMIT or ROLLBACK, sent as ordinary statements — across a lost
// connection: against the fake server that keeps a transaction per
// connection, asserted by what reached it and by the rows that exist after.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Skaidb.Tests;

public class HandWrittenTransactionTests
{
    private const string Count = "SELECT count(*) FROM notes";
    private const string Lost = "connection lost inside a transaction";

    private static string Row(int id) => $"INSERT INTO notes (id, body) VALUES ({id}, 'row {id}')";

    private static string Cs(FakeServer srv, string extra = "") =>
        $"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5;{extra}";

    private static SkaidbConnection Connect(FakeServer srv, string extra = "")
    {
        var c = new SkaidbConnection(Cs(srv, extra));
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

    private static long Rows(SkaidbConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = Count;
        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>The Async methods complete synchronously (the driver's I/O is synchronous).</summary>
    private static void Wait(System.Threading.Tasks.Task<int> done)
    {
        done.GetAwaiter().GetResult();
    }

    /// <summary>
    /// The connection is lost with the transaction open: while it sits idle
    /// (closed, or half-closed as a real network shows it), or under the
    /// statement <paramref name="under"/>, which the server takes and never
    /// answers.
    /// </summary>
    private static void Lose(FakeServer srv, Sessions d, SkaidbConnection c, string how, string under)
    {
        switch (how)
        {
            case "closed while idle":
                srv.CloseAll(seenBy: c);
                break;
            case "half-closed while idle":
                srv.HalfCloseAll(c);
                break;
            default:
                d.Drop = sql => sql == under;
                break;
        }
    }

    /// <summary>Every way a statement of the transaction can be run, each a write that must not be sent.</summary>
    private static (string Way, Action Run)[] Ways(SkaidbConnection c)
    {
        const string bound = "INSERT INTO notes (id, body) VALUES (?, ?)";
        var rows = new List<IReadOnlyList<object?>> { new object?[] { 31L, "x" }, new object?[] { 32L, "y" } };
        return new (string, Action)[]
        {
            ("ExecuteNonQuery, text", () => Exec(c, Row(3))),
            ("ExecuteNonQuery, prepared", () => Exec(c, bound, 33L, "z")),
            ("ExecuteScalar", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = Row(34);
                cmd.ExecuteScalar();
            }),
            ("ExecuteReader", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = Row(35);
                using var r = cmd.ExecuteReader();
            }),
            ("ExecuteNonQueryAsync", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = Row(36);
                Wait(cmd.ExecuteNonQueryAsync());
            }),
            ("Prepare", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = bound;
                cmd.Parameters.Add(37L);
                cmd.Parameters.Add("p");
                cmd.Prepare();
            }),
            ("ExecuteBatch", () =>
            {
                using var cmd = c.CreateCommand();
                cmd.CommandText = bound;
                cmd.ExecuteBatch(rows);
            }),
            ("DbBatch", () =>
            {
                using var batch = c.CreateBatch();
                batch.BatchCommands.Add(Row(38));
                batch.BatchCommands.Add(Row(39));
                batch.ExecuteNonQuery();
            }),
            ("Stream", () => c.Stream(Row(40)).ToList()),
            ("Stream, parameters", () => c.Stream(bound, new object?[] { 41L, "s" }).ToList()),
            ("ChangeDatabase", () => c.ChangeDatabase("other")),
            ("a second BEGIN", () => Exec(c, "BEGIN")),
            ("a SET", () => Exec(c, "SET SCAN BUDGET ROWS 5")),
        };
    }

    // ---- the scenario -------------------------------------------------------------------

    [Theory]
    [InlineData("closed while idle", "COMMIT")]
    [InlineData("half-closed while idle", "COMMIT")]
    [InlineData("under the statement", "COMMIT")]
    [InlineData("closed while idle", "ROLLBACK")]
    [InlineData("half-closed while idle", "ROLLBACK")]
    [InlineData("under the statement", "ROLLBACK")]
    public void A_connection_lost_inside_a_hand_written_transaction_is_not_replaced_until_the_transaction_is_ended(
        string how, string end)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Assert.Equal(0, Exec(c, "BEGIN"));
        Assert.Equal(1, Exec(c, Row(1)));
        var socket = srv.Accepted.Single();
        Lose(srv, d, c, how, under: Row(2));

        // Row 2: the statement under which the connection is lost, or the
        // one that finds it lost. It fails, and it is not sent again.
        var two = Assert.Throws<SkaidbException>(() => Exec(c, Row(2)));
        Assert.Contains(how == "under the statement" ? "connection closed by server" : Lost, two.Message);
        // Row 3, whichever way it is run: a new connection has no
        // transaction open, so it would be applied there on its own.
        foreach (var (way, run) in Ways(c))
            Assert.Contains(Lost, Assert.Throws<SkaidbException>(run).Message);
        var sent = how == "under the statement" ? new[] { "BEGIN", Row(1), Row(2) } : new[] { "BEGIN", Row(1) };
        Assert.Equal(sent, d.Since(0));
        Assert.Equal(1, srv.Connections);                                        // nothing was dialed
        Assert.False(c.IsUsable);
        Assert.True(c.IsOpen);

        // The application ends its transaction, and the driver answers for
        // a server that has none of it left.
        if (end == "COMMIT")
        {
            var commit = Assert.Throws<SkaidbException>(() => Exec(c, "COMMIT"));
            Assert.Contains(Lost, commit.Message);
            Assert.Contains("nothing was committed", commit.Message);
        }
        else
        {
            Assert.Equal(0, Exec(c, "ROLLBACK"));                                // it is rolled back: the server discarded it
        }
        Assert.Equal(sent, d.Since(0));                                          // neither was sent
        Assert.Equal(1, srv.Connections);
        if (how == "half-closed while idle") Assert.Equal(0, socket.ReceivedSinceHalfClose);

        // Ended: the next statement re-dials as it always does, and finds
        // none of the three rows.
        Assert.Equal(0L, Rows(c));
        Assert.Equal(2, srv.Connections);
        Assert.True(c.IsUsable);
        Assert.Empty(d.Ids("default"));
        Assert.Equal(Count, d.Log[^1].Sql);
        Assert.False(d.Log[^1].InTransaction);
        Assert.Same(srv.Accepted[1], d.Log[^1].Socket);
    }

    [Fact]
    public void Without_a_loss_a_hand_written_transaction_is_what_it_was()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        using var other = Connect(srv);
        Assert.Equal(0, Exec(c, "BEGIN"));
        Assert.Equal(1, Exec(c, Row(1)));
        Assert.Equal(1L, Rows(c));                                               // its own row
        Assert.Equal(0L, Rows(other));                                           // nobody else's yet
        Assert.Equal(0, Exec(c, "COMMIT"));
        Assert.Equal(1L, Rows(other));
        Assert.Equal(0, Exec(c, "BEGIN"));
        Assert.Equal(1, Exec(c, Row(2)));
        Assert.Equal(0, Exec(c, "ROLLBACK"));
        Assert.Equal(new long[] { 1 }, d.Ids("default"));
        Assert.Equal(new[] { "BEGIN", Row(1), Count, "COMMIT", "BEGIN", Row(2), "ROLLBACK" },
            d.Log.Where(l => l.Socket == srv.Accepted[0]).Select(l => l.Sql));
        Assert.Equal(new[] { false, true, true, true, false, true, true },
            d.Log.Where(l => l.Socket == srv.Accepted[0]).Select(l => l.InTransaction));

        // Both are over: a connection lost now is replaced like any other.
        srv.CloseAll(c, other);
        Assert.Equal(1, Exec(c, Row(3)));
        Assert.Equal(new long[] { 1, 3 }, d.Ids("default"));
        Assert.Equal(3, srv.Connections);
    }

    // ---- what opens the noted transaction, and what ends it ---------------------------------

    [Theory]
    [InlineData("BEGIN")]
    [InlineData("begin;")]
    [InlineData("BEGIN TRANSACTION")]
    [InlineData("  Begin\tTransaction ;\n")]
    [InlineData("BEGIN -- the transfer")]
    [InlineData("-- the transfer\nBEGIN")]
    [InlineData("BEGIN ATOMIC")]
    [InlineData("BEGIN ATOMIC PARTITION (tenant_id = 't;1')")]
    public void A_BEGIN_the_server_accepted_opens_it_however_it_is_written(string begin)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Assert.Equal(0, Exec(c, begin));
        Assert.Equal(1, Exec(c, Row(1)));
        srv.CloseAll(seenBy: c);
        Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(c, Row(2))).Message);
        Assert.Equal(1, srv.Connections);
        Assert.Equal(0, Exec(c, "ROLLBACK"));
        Assert.Equal(0L, Rows(c));
        Assert.Empty(d.Ids("default"));
    }

    [Theory]
    [InlineData("ExecuteScalar")]
    [InlineData("ExecuteReader")]
    [InlineData("ExecuteNonQueryAsync")]
    [InlineData("DbBatch")]
    [InlineData("Stream")]
    public void A_BEGIN_opens_it_whichever_way_the_statement_is_run(string way)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        using var cmd = c.CreateCommand();
        cmd.CommandText = "BEGIN";
        switch (way)
        {
            case "ExecuteScalar":
                Assert.Null(cmd.ExecuteScalar());
                break;
            case "ExecuteReader":
                using (var r = cmd.ExecuteReader()) Assert.False(r.Read());
                break;
            case "ExecuteNonQueryAsync":
                Wait(cmd.ExecuteNonQueryAsync());
                break;
            case "DbBatch":
                using (var batch = c.CreateBatch())
                {
                    batch.BatchCommands.Add("BEGIN");
                    batch.BatchCommands.Add(Row(1));
                    Assert.Equal(1, batch.ExecuteNonQuery());
                }
                break;
            default:
                Assert.Empty(c.Stream("BEGIN").ToList());
                break;
        }
        srv.CloseAll(seenBy: c);
        Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(c, Row(2))).Message);
        // And each way to send the ROLLBACK is answered without sending it.
        int logged = d.Log.Length;
        switch (way)
        {
            case "ExecuteScalar":
                cmd.CommandText = "ROLLBACK";
                Assert.Null(cmd.ExecuteScalar());
                break;
            case "ExecuteReader":
                cmd.CommandText = "ROLLBACK";
                using (var r = cmd.ExecuteReader()) Assert.False(r.Read());
                break;
            case "ExecuteNonQueryAsync":
                cmd.CommandText = "ROLLBACK";
                Wait(cmd.ExecuteNonQueryAsync());
                break;
            case "DbBatch":
                using (var batch = c.CreateBatch())
                {
                    batch.BatchCommands.Add("ROLLBACK");
                    Assert.Equal(0, batch.ExecuteNonQuery());
                }
                break;
            default:
                Assert.Empty(c.Stream("ROLLBACK").ToList());
                break;
        }
        Assert.Equal(logged, d.Log.Length);
        Assert.Equal(1, srv.Connections);
        Assert.Equal(0L, Rows(c));
        Assert.Equal(2, srv.Connections);
    }

    [Theory]
    [InlineData("COMMIT", true)]
    [InlineData("commit;", true)]
    [InlineData("COMMIT TRANSACTION", true)]
    [InlineData("Commit Transaction ; -- done", true)]
    [InlineData("ROLLBACK", false)]
    [InlineData("rollback transaction", false)]
    [InlineData("-- give up\nROLLBACK;", false)]
    public void A_COMMIT_or_ROLLBACK_the_server_answered_ends_it_however_it_is_written(string end, bool committed)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        Assert.Equal(0, Exec(c, end));
        // Over: the connection lost now is replaced, and the statement runs.
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, Exec(c, Row(2)));
        Assert.Equal(2, srv.Connections);
        Assert.Equal(committed ? new long[] { 1, 2 } : new long[] { 2 }, d.Ids("default"));
    }

    [Theory]
    [InlineData("ExecuteNonQuery")]
    [InlineData("Stream")]
    public void A_COMMIT_the_server_refused_has_ended_it_too(string way)
    {
        const string conflict = "transaction conflict: a row of table notes was changed by another writer after the "
                                + "transaction began — nothing was committed; run the transaction again";
        var d = new Sessions { Refuse = (sql, _) => sql == "COMMIT" ? conflict : null };
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        var refused = Assert.Throws<SkaidbTransactionConflictException>(() =>
        {
            if (way == "Stream") c.Stream("COMMIT").ToList();
            else Exec(c, "COMMIT");
        });
        Assert.Equal(conflict, refused.Message);
        Assert.True(c.IsUsable);
        // Rule 6: the transaction is over. A connection lost now is not a
        // lost transaction.
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, Exec(c, Row(2)));
        Assert.Equal(new long[] { 2 }, d.Ids("default"));
        Assert.Equal(2, srv.Connections);
    }

    [Fact]
    public void A_BEGIN_the_server_refused_opens_nothing()
    {
        // A cluster refuses plain BEGIN. Nothing is open, so a connection
        // lost afterwards is no lost transaction.
        const string refusal = "unsupported: plain BEGIN is a standalone statement; use BEGIN ATOMIC";
        var d = new Sessions { Refuse = (sql, _) => sql == "BEGIN" ? refusal : null };
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Assert.Equal(refusal, Assert.Throws<SkaidbException>(() => Exec(c, "BEGIN")).Message);
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, Exec(c, Row(1)));
        Assert.Equal(2, srv.Connections);
        Assert.Equal(new long[] { 1 }, d.Ids("default"));
    }

    [Fact]
    public void A_statement_the_server_refused_that_is_no_COMMIT_leaves_it_open()
    {
        const string refusal = "parse error: unexpected token \"Ident(\\\"WORK\\\")\", expected end of input";
        var d = new Sessions { Refuse = (sql, _) => sql == "COMMIT WORK" ? refusal : null };
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        Assert.Equal(refusal, Assert.Throws<SkaidbException>(() => Exec(c, "COMMIT WORK")).Message);
        srv.CloseAll(seenBy: c);
        Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(c, Row(2))).Message);
        Assert.Equal(1, srv.Connections);
    }

    [Theory]
    [InlineData("SELECT 'BEGIN'")]
    [InlineData("-- BEGIN")]
    [InlineData("BEGINNING")]
    [InlineData("EXPLAIN BEGIN")]
    [InlineData("COMMIT")]                                                       // with nothing open: it opens nothing either
    public void A_statement_that_is_no_BEGIN_opens_nothing(string sql)
    {
        // Whatever the server makes of these, none is a BEGIN it accepted.
        var d = new Sessions();
        using var srv = new FakeServer((req, conn) => req.Op == 1 && req.Sql == sql ? new[] { F.Ddl() } : d.Handle(req, conn));
        using var c = Connect(srv);
        Exec(c, sql);
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, Exec(c, Row(1)));
        Assert.Equal(2, srv.Connections);
    }

    // ---- lost under the COMMIT, under the ROLLBACK -----------------------------------------

    [Fact]
    public void A_connection_lost_under_the_COMMIT_itself_says_the_outcome_is_unknown()
    {
        var d = new Sessions { Drop = sql => sql == "COMMIT" };
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        var e = Assert.Throws<SkaidbException>(() => Exec(c, "COMMIT"));
        // The server had the COMMIT and its answer never came: it may have
        // applied it. Neither "committed" nor "nothing was committed".
        Assert.Contains("outcome is unknown", e.Message);
        Assert.Contains("connection closed by server", e.Message);
        Assert.DoesNotContain("nothing was committed", e.Message);
        Assert.NotNull(e.InnerException);
        Assert.Equal(new[] { "BEGIN", Row(1), "COMMIT" }, d.Since(0));
        // The transaction is over either way: the next statement re-dials.
        d.Drop = null;
        Assert.Equal(0L, Rows(c));
        Assert.Equal(2, srv.Connections);
    }

    [Fact]
    public void A_connection_lost_under_the_ROLLBACK_itself_is_a_transaction_rolled_back()
    {
        var d = new Sessions { Drop = sql => sql == "ROLLBACK" };
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        // Whether the server read the ROLLBACK or only lost the connection,
        // the transaction is discarded: that is what the caller asked for.
        Assert.Equal(0, Exec(c, "ROLLBACK"));
        Assert.False(c.IsUsable);
        d.Drop = null;
        Assert.Equal(0L, Rows(c));
        Assert.Equal(2, srv.Connections);
        Assert.Empty(d.Ids("default"));
    }

    // ---- the helper, a pool, Close() --------------------------------------------------------

    [Fact]
    public void The_helper_s_transaction_is_noted_once_and_is_over_when_the_helper_returns()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Assert.Equal(1, c.Transaction(conn => Exec(conn, Row(1))));
        Assert.Equal(new[] { "BEGIN", Row(1), "COMMIT" }, d.Since(0));           // one BEGIN, one COMMIT
        srv.CloseAll(seenBy: c);
        Assert.Equal(1, Exec(c, Row(2)));                                        // nothing is left open: it re-dials
        Assert.Equal(2, srv.Connections);

        // Lost inside the helper, the body swallowing every error: the
        // helper reports the loss as before, and afterwards the connection
        // is not held by a transaction nobody will end.
        int logged = d.Log.Length;
        bool returned = false;
        var lost = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            Exec(conn, Row(3));
            srv.CloseAll(seenBy: conn);
            Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(conn, Row(4))).Message);
            // A ROLLBACK of the body's own is answered, and still nothing
            // of the body goes to a new connection: the helper is not done.
            Assert.Equal(0, Exec(conn, "ROLLBACK"));
            Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(conn, Row(5))).Message);
            returned = true;
        }));
        Assert.True(returned);
        Assert.Contains(Lost, lost.Message);                                     // the helper's COMMIT, not sent
        Assert.Equal(new[] { "BEGIN", Row(3) }, d.Since(logged));
        Assert.Equal(2, srv.Connections);
        Assert.Equal(2L, Rows(c));                                               // rows 1 and 2
        Assert.Equal(3, srv.Connections);
        Assert.Equal(new long[] { 1, 2 }, d.Ids("default"));

        // A body that only swallows: the helper's COMMIT is the one that
        // says what became of the transaction.
        lost = Assert.Throws<SkaidbException>(() => c.Transaction(conn =>
        {
            Exec(conn, Row(6));
            srv.CloseAll(seenBy: conn);
            Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(conn, Row(7))).Message);
        }));
        Assert.Contains(Lost, lost.Message);
        Assert.Contains("nothing was committed", lost.Message);
        Assert.Equal(2L, Rows(c));
    }

    [Fact]
    public void A_hand_written_transaction_on_a_pooled_connection_is_guarded_the_same_way()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv));
        var c = pool.Acquire();
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        srv.CloseAll(seenBy: c);
        Assert.Contains(Lost, Assert.Throws<SkaidbException>(() => Exec(c, Row(2))).Message);
        Assert.Contains("nothing was committed", Assert.Throws<SkaidbException>(() => Exec(c, "COMMIT")).Message);
        Assert.Equal(1, srv.Connections);
        Assert.Equal(1, Exec(c, Row(3)));                                        // ended: a statement of its own, on a new connection
        Assert.Equal(2, srv.Connections);
        pool.Release(c);
        Assert.Equal(new long[] { 3 }, d.Ids("default"));
    }

    [Fact]
    public void Close_ends_a_hand_written_transaction_with_its_session()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "BEGIN");
        Exec(c, Row(1));
        // The application closes the connection itself: it knows the
        // session is over, and the server discards the transaction with it.
        c.Close();
        c.Open();
        Assert.Equal(0L, Rows(c));
        Assert.Equal(1, Exec(c, Row(2)));
        Assert.Equal(new long[] { 2 }, d.Ids("default"));
        Assert.Equal(2, srv.Connections);
        Assert.False(d.Log[^1].InTransaction);
    }
}
