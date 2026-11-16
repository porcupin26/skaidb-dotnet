// The session settings an application makes with SET CONSISTENCY and SET
// SCAN BUDGET, across a re-dial: against the fake server that keeps them per
// connection, asserted by the settings each statement actually ran under.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Skaidb.Tests;

public class SessionSettingsTests
{
    private const string Scan = "SELECT id FROM notes";
    private const string Count = "SELECT count(*) FROM notes";

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

    private static List<long> Ids(SkaidbConnection c)
    {
        var ids = new List<long>();
        using var cmd = c.CreateCommand();
        cmd.CommandText = Scan;
        using var r = cmd.ExecuteReader();
        while (r.Read()) ids.Add(r.GetInt64(0));
        return ids;
    }

    /// <summary>The Async methods complete synchronously (the driver's I/O is synchronous).</summary>
    private static void Wait(System.Threading.Tasks.Task<int> done)
    {
        done.GetAwaiter().GetResult();
    }

    private static void BudgetExceeded(SkaidbConnection c) =>
        Assert.StartsWith("resource limit: scan budget exceeded",
            Assert.Throws<SkaidbException>(() => Ids(c)).Message);

    // ---- the settings are made again -----------------------------------------------------

    [Theory]
    [InlineData("SET SCAN BUDGET ROWS 1", "SET SCAN BUDGET ROWS 1")]
    [InlineData("set scan budget rows 1 bytes 100000", "set scan budget rows 1 bytes 100000")]
    [InlineData("SET SCAN BUDGET BYTES 100000 ROWS 1;", "SET SCAN BUDGET BYTES 100000 ROWS 1")]
    [InlineData("SET SCAN BUDGET ROWS 1 -- one row at most", "SET SCAN BUDGET ROWS 1")]
    [InlineData("-- tighten\n  Set Scan Budget Rows 1 ;\n", "Set Scan Budget Rows 1")]
    public void A_scan_budget_set_with_a_statement_is_set_again_after_a_re_dial(string set, string sentAgain)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv, "Database=app");
        Assert.Equal(0, Exec(c, set));
        BudgetExceeded(c);                                                       // app.notes has 3 rows
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);

        // The limit the application set is a property of its session, not
        // of the socket the session happened to be on: the same scan fails
        // the same way on the connection the driver replaced it with.
        BudgetExceeded(c);
        Assert.Equal(2, srv.Connections);
        Assert.Equal(new[] { "USE \"app\"", sentAgain, Scan }, d.Since(logged)); // after the database, before the statement
        Assert.Single(d.Log.Skip(logged).Select(l => l.Socket).Distinct());
        Assert.Equal(1L, d.Log[^1].Budget);
        Assert.Equal("app", d.Log[^1].Database);
    }

    [Theory]
    [InlineData("SET CONSISTENCY ALL", "SET CONSISTENCY ALL", "ALL")]
    [InlineData("set consistency one;", "set consistency one", "ONE")]
    [InlineData("SET CONSISTENCY ALL -- every replica", "SET CONSISTENCY ALL", "ALL")]
    [InlineData("-- reads\nSET\tCONSISTENCY\n  ALL", "SET\tCONSISTENCY\n  ALL", "ALL")]
    public void A_consistency_override_set_with_a_statement_is_set_again_after_a_re_dial(string set, string sentAgain, string level)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);                                              // requests carry QUORUM
        Assert.Equal(0L, Rows(c));
        Assert.Equal("QUORUM", d.Log[^1].Level);
        Assert.Equal(0, Exec(c, set));
        Assert.Equal(0L, Rows(c));
        Assert.Equal(level, d.Log[^1].Level);
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);

        Assert.Equal(0L, Rows(c));
        Assert.Equal(new[] { sentAgain, Count }, d.Since(logged));
        Assert.Equal(level, d.Log[^1].Level);                                    // not back at the request's QUORUM
        Assert.Equal(1, Exec(c, "INSERT INTO notes (id, body) VALUES (9, 'after')"));
        Assert.Equal(level, d.Log[^1].Level);
        Assert.Equal(2, srv.Connections);
    }

    [Fact]
    public void Both_settings_are_made_again_behind_the_database_consistency_first()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "SET SCAN BUDGET ROWS 1");                                       // set in the other order
        Exec(c, "SET CONSISTENCY ALL");
        Exec(c, "USE app");
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        BudgetExceeded(c);
        Assert.Equal(new[] { "USE \"app\"", "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1", Scan }, d.Since(logged));
        Assert.Equal(new Sessions.Ran(Scan, "app", "ALL", 1L, false, srv.Accepted[1]), d.Log[^1]);

        // A second loss: once each, again.
        logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        Assert.Equal(3L, Rows(c));
        Assert.Equal(new[] { "USE \"app\"", "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1", Count }, d.Since(logged));
        Assert.Equal(3, srv.Connections);
    }

    [Fact]
    public void The_last_setting_of_each_kind_is_the_one_made_again_and_a_refused_SET_changes_nothing()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv, "Database=other");                            // other.notes has 7 rows
        Exec(c, "SET SCAN BUDGET ROWS 1");
        Exec(c, "SET SCAN BUDGET ROWS 5 BYTES 4096");
        Exec(c, "SET CONSISTENCY ALL");
        Exec(c, "SET CONSISTENCY ONE");
        // The server said no: the session is as it was, and so is what the driver knows of it.
        Assert.StartsWith("parse error", Assert.Throws<SkaidbException>(() => Exec(c, "SET CONSISTENCY TWO")).Message);
        Assert.StartsWith("parse error", Assert.Throws<SkaidbException>(() => Exec(c, "SET SCAN BUDGET ROWS 0")).Message);
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        BudgetExceeded(c);
        Assert.Equal(new[] { "USE \"other\"", "SET CONSISTENCY ONE", "SET SCAN BUDGET ROWS 5 BYTES 4096", Scan }, d.Since(logged));
        Assert.Equal(5L, d.Log[^1].Budget);
        Assert.Equal("ONE", d.Log[^1].Level);
    }

    [Fact]
    public void A_setting_cleared_with_DEFAULT_is_not_made_again()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv, "Database=app");
        Exec(c, "SET SCAN BUDGET ROWS 1");
        Exec(c, "SET CONSISTENCY ALL");
        Exec(c, "SET SCAN BUDGET DEFAULT");
        Exec(c, "set consistency default; -- back to each request's own");
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        Assert.Equal(new[] { 1L, 2L, 3L }, Ids(c));
        Assert.Equal(new[] { "USE \"app\"", Scan }, d.Since(logged));
        Assert.Equal(new Sessions.Ran(Scan, "app", "QUORUM", null, false, srv.Accepted[1]), d.Log[^1]);

        // One cleared, the other kept.
        Exec(c, "SET SCAN BUDGET ROWS 1");
        Exec(c, "SET CONSISTENCY ALL");
        Exec(c, "SET SCAN BUDGET DEFAULT");
        logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        Assert.Equal(new[] { 1L, 2L, 3L }, Ids(c));
        Assert.Equal(new[] { "USE \"app\"", "SET CONSISTENCY ALL", Scan }, d.Since(logged));
    }

    [Fact]
    public void A_setting_refused_on_the_new_connection_fails_the_statement_which_never_runs_without_it()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv, "Database=app");
        Exec(c, "SET CONSISTENCY ALL");
        Exec(c, "SET SCAN BUDGET ROWS 1");
        var first = srv.Accepted.Single();
        string? refusal = "permission denied: SET SCAN BUDGET";
        d.Refuse = (sql, conn) => conn != first && sql.StartsWith("SET SCAN BUDGET", StringComparison.Ordinal) ? refusal : null;
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);

        // An unlimited scan is not what the application asked for: the
        // statement fails with the setting's error and is not sent.
        Assert.Equal(refusal, Assert.Throws<SkaidbException>(() => Ids(c)).Message);
        Assert.Equal(refusal, Assert.Throws<SkaidbException>(() => Exec(c, "INSERT INTO notes (id, body) VALUES (9, 'x')")).Message);
        Assert.False(c.IsUsable);
        Assert.True(c.IsOpen);
        Assert.Equal(new[]
        {
            "USE \"app\"", "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1",       // two dials, each stopped at the setting
            "USE \"app\"", "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1",
        }, d.Since(logged));
        Assert.Equal(3, d.Rows("app"));

        // Accepted again: the next statement dials, sets both, and runs under them.
        refusal = null;
        BudgetExceeded(c);
        Assert.Equal(new Sessions.Ran(Scan, "app", "ALL", 1L, false, srv.Accepted[^1]), d.Log[^1]);
        Assert.True(c.IsUsable);
    }

    [Theory]
    [InlineData("ExecuteScalar")]
    [InlineData("ExecuteReader")]
    [InlineData("ExecuteNonQueryAsync")]
    [InlineData("DbBatch")]
    [InlineData("Stream")]
    [InlineData("a parameter")]
    public void A_setting_is_noted_whichever_way_the_statement_is_run(string way)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv, "Database=app");
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SET SCAN BUDGET ROWS 1";
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
                    batch.BatchCommands.Add("SET SCAN BUDGET ROWS 1");
                    batch.BatchCommands.Add("SET CONSISTENCY ALL");
                    batch.ExecuteNonQuery();
                }
                break;
            case "Stream":
                Assert.Empty(c.Stream("SET SCAN BUDGET ROWS 1").ToList());
                break;
            default:
                // A session statement cannot be prepared, so its value goes
                // out as a literal: that text is the one to send again, not
                // the one with the placeholder.
                cmd.CommandText = "SET SCAN BUDGET ROWS @rows";
                cmd.Parameters.AddWithValue("@rows", 1);
                Assert.Equal(0, cmd.ExecuteNonQuery());
                break;
        }
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        BudgetExceeded(c);
        var since = d.Since(logged);
        Assert.Equal("SET SCAN BUDGET ROWS 1", since[^2]);
        Assert.Equal(way == "DbBatch" ? 4 : 3, since.Length);
        Assert.Equal(1L, d.Log[^1].Budget);
    }

    [Theory]
    [InlineData("SET SCAN BUDGET ROWS 1; SELECT 1")]                             // two statements
    [InlineData("SELECT 'SET SCAN BUDGET ROWS 1'")]
    [InlineData("-- SET SCAN BUDGET ROWS 1")]                                    // all of it is a comment
    [InlineData("SELECT 1 -- SET CONSISTENCY ALL")]
    [InlineData("EXPLAIN SET SCAN BUDGET ROWS 1")]
    [InlineData("SET SCAN BUDGETS ROWS 1")]
    [InlineData("SET SCANNER BUDGET ROWS 1")]
    [InlineData("SET SCAN")]
    [InlineData("SET CONSISTENCYALL")]
    [InlineData("SETCONSISTENCY ALL")]
    [InlineData("SET CONFIG consistency = 'ALL'")]
    [InlineData("RESET CONSISTENCY ALL")]
    public void A_statement_that_is_not_exactly_one_SET_of_a_session_setting_is_left_alone(string sql)
    {
        // Whatever the server makes of these, the driver takes nothing from
        // a text it cannot read as one SET CONSISTENCY or SET SCAN BUDGET.
        var d = new Sessions();
        using var srv = new FakeServer((req, conn) => req.Op == 1 && req.Sql == sql ? new[] { F.Ddl() } : d.Handle(req, conn));
        using var c = Connect(srv, "Database=app");
        Exec(c, sql);
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        Assert.Equal(new[] { 1L, 2L, 3L }, Ids(c));
        Assert.Equal(new[] { "USE \"app\"", Scan }, d.Since(logged));
    }

    // ---- a pool, Close() -----------------------------------------------------------------

    [Fact]
    public void A_pooled_connection_that_re_dials_in_its_borrower_s_hands_keeps_the_borrower_s_settings()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv, "Database=app"));
        var c = pool.Acquire();
        Exec(c, "SET SCAN BUDGET ROWS 1");
        Exec(c, "SET CONSISTENCY ALL");
        BudgetExceeded(c);
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        BudgetExceeded(c);                                                       // still the borrower's session
        Assert.Equal(new[] { "USE \"app\"", "SET CONSISTENCY ALL", "SET SCAN BUDGET ROWS 1", Scan }, d.Since(logged));
        Assert.Equal("ALL", d.Log[^1].Level);
        Assert.Equal(2, srv.Connections);
        pool.Release(c);
    }

    [Fact]
    public void Close_ends_the_session_and_its_settings_and_Open_starts_another_in_the_connection_s_database()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var c = Connect(srv);
        Exec(c, "USE app");
        Exec(c, "SET SCAN BUDGET ROWS 1");
        Exec(c, "SET CONSISTENCY ALL");
        int logged = d.Log.Length;
        // A re-dial is the driver replacing a socket under a session the
        // application still thinks it has. Close() is the application
        // ending the session: the next Open() is a new one, in the
        // connection's Database, with nothing set.
        c.Close();
        c.Open();
        Assert.Equal(new[] { 1L, 2L, 3L }, Ids(c));
        Assert.Equal(new[] { "USE \"app\"", Scan }, d.Since(logged));
        Assert.Equal(new Sessions.Ran(Scan, "app", "QUORUM", null, false, srv.Accepted[1]), d.Log[^1]);
        logged = d.Log.Length;
        srv.CloseAll(seenBy: c);
        Assert.Equal(new[] { 1L, 2L, 3L }, Ids(c));
        Assert.Equal(new[] { "USE \"app\"", Scan }, d.Since(logged));
    }
}
