// What one borrower of a pooled connection leaves on its session — the
// database, a consistency override, a scan budget, an open transaction — and
// what the next borrower finds: against the fake server that keeps sessions,
// asserted by the session each statement actually ran in.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Skaidb.Tests;

public class PoolSessionTests
{
    private const string Scan = "SELECT id FROM notes";
    private const string Count = "SELECT count(*) FROM notes";

    private static string Cs(FakeServer srv, string extra = "") =>
        $"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5;{extra}";

    private static int Exec(SkaidbConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
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

    /// <summary>A pool of one idle connection, so two borrowers in a row get the same one.</summary>
    private static SkaidbConnectionPool Pool(FakeServer srv, string database) =>
        new SkaidbConnectionPool(Cs(srv, database.Length > 0 ? "Database=" + database : ""), maxsize: 1);

    private static string Use(string database) => "USE \"" + database + "\"";

    // ---- the scenario -------------------------------------------------------------------

    [Theory]
    [InlineData("")]                                                             // a pool that names no database: default
    [InlineData("app")]
    public void The_next_borrower_gets_the_pool_s_session_not_the_one_the_last_borrower_left(string database)
    {
        string home = database.Length > 0 ? database : "default";
        var d = new Sessions().With("default", 1, 2);                            // so a scan in default has something to refuse
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, database);
        var mine = d.Ids(home).ToList();

        // Borrower 1 moves the session and tightens it for its own work.
        pool.WithConnection(c =>
        {
            Exec(c, "USE other");
            Exec(c, "SET SCAN BUDGET ROWS 1");
            Exec(c, "SET CONSISTENCY ALL");
            Assert.Equal(7L, Rows(c));
            Assert.StartsWith("resource limit: scan budget exceeded", Assert.Throws<SkaidbException>(() => Ids(c)).Message);
            return 0;
        });
        int logged = d.Log.Length;

        // Borrower 2 knows nothing of that. Its statements run in the
        // pool's database, at their own level, with no limit of someone
        // else's.
        pool.WithConnection(c =>
        {
            Assert.Equal(database, c.Database);
            Assert.Equal(mine.Count, Rows(c));
            Assert.Equal(mine, Ids(c));                                          // not refused for a scan budget
            return 0;
        });
        Assert.Equal(new Sessions.Ran(Count, home, "QUORUM", null, false, srv.Accepted[0]), d.Log[^2]);
        Assert.Equal(new Sessions.Ran(Scan, home, "QUORUM", null, false, srv.Accepted[0]), d.Log[^1]);
        Assert.Equal(1, srv.Connections);                                        // the same connection, put back
        // Put back when it was returned, before anyone could get it.
        Assert.Equal(new[] { Use(home), "SET SCAN BUDGET DEFAULT", "SET CONSISTENCY DEFAULT" },
            d.Log.Skip(logged - 3).Take(3).Select(l => l.Sql));
        Assert.Equal(new[] { Count, Scan }, d.Since(logged));
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
    }

    [Fact]
    public void A_borrower_that_changed_nothing_costs_no_round_trip()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, "app");
        using var bare = Pool(srv, "");
        foreach (var p in new[] { pool, bare })
        {
            for (int i = 0; i < 3; i++)
            {
                p.WithConnection(c =>
                {
                    Rows(c);
                    Exec(c, "INSERT INTO notes (id, body) VALUES (50, 'x')");
                    Exec(c, "BEGIN");
                    Exec(c, "COMMIT");                                           // ended by the borrower: nothing is left open
                    Exec(c, "SET SCAN BUDGET ROWS 9");
                    Exec(c, "SET SCAN BUDGET DEFAULT");                          // cleared by the borrower
                    return 0;
                });
            }
        }
        Assert.Equal(2, srv.Connections);
        Assert.DoesNotContain(d.Log, l => l.Sql.StartsWith("USE", StringComparison.Ordinal) && l.Socket == srv.Accepted[1]);
        Assert.Equal(1, d.Log.Count(l => l.Sql.StartsWith("USE", StringComparison.Ordinal)));   // the dial of the pool with a database
        Assert.DoesNotContain(d.Log, l => l.Sql == "ROLLBACK" || l.Sql == "SET CONSISTENCY DEFAULT");
        Assert.Equal(6, d.Log.Count(l => l.Sql == "SET SCAN BUDGET DEFAULT"));   // the borrowers' own, nothing added
    }

    [Theory]
    [InlineData("USE other", "USE \"default\"")]
    [InlineData("SET SCAN BUDGET ROWS 1", "SET SCAN BUDGET DEFAULT")]
    [InlineData("SET CONSISTENCY ONE", "SET CONSISTENCY DEFAULT")]
    [InlineData("BEGIN", "ROLLBACK")]
    public void Only_what_the_borrower_changed_is_put_back(string changed, string putBack)
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, "");
        pool.WithConnection(c => Exec(c, changed));
        Assert.Equal(new[] { changed, putBack }, d.Since(0));
        pool.WithConnection(Rows);
        Assert.Equal(new Sessions.Ran(Count, "default", "QUORUM", null, false, srv.Accepted[0]), d.Log[^1]);
        Assert.Equal(new[] { changed, putBack, Count }, d.Since(0));
        // And nothing again: what was put back is not put back twice.
        pool.WithConnection(Rows);
        Assert.Equal(new[] { changed, putBack, Count, Count }, d.Since(0));
        Assert.Equal(1, srv.Connections);
    }

    [Fact]
    public void A_borrower_s_USE_of_the_database_the_pool_is_in_anyway_needs_no_statement_to_put_back()
    {
        // A pool that names no database is in default. A borrower that says
        // so out loud has moved nothing: no USE is sent for it, and the next
        // borrower's connection is the pool's again, by name too.
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, "");
        pool.WithConnection(c =>
        {
            Exec(c, "USE default");
            Assert.Equal("default", c.Database);
            return 0;
        });
        Assert.Equal(new[] { "USE default" }, d.Since(0));
        pool.WithConnection(c =>
        {
            Assert.Equal("", c.Database);
            return Rows(c);
        });
        // Lost in a borrower's hands: nothing of the first borrower's USE comes back.
        var held = pool.Acquire();
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: held);
        Rows(held);
        Assert.Equal(new[] { Count }, d.Since(logged));
        pool.Release(held);
    }

    // ---- a transaction left open ----------------------------------------------------------

    [Fact]
    public void A_transaction_a_borrower_left_open_is_rolled_back_before_the_connection_is_handed_on()
    {
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, "app");
        pool.WithConnection(c =>
        {
            Exec(c, "BEGIN");
            Exec(c, "INSERT INTO notes (id, body) VALUES (60, 'never committed')");
            Exec(c, "USE other");
            Exec(c, "SET CONSISTENCY ALL");
            return 0;                                                            // and walks away
        });
        // First the ROLLBACK, then the session.
        Assert.Equal(new[] { "ROLLBACK", Use("app"), "SET CONSISTENCY DEFAULT" }, d.Log.TakeLast(3).Select(l => l.Sql));
        pool.WithConnection(c =>
        {
            Assert.Equal(3L, Rows(c));                                           // not the 4 of a transaction it would have joined
            Exec(c, "INSERT INTO notes (id, body) VALUES (61, 'its own')");
            return 0;
        });
        Assert.False(d.Log[^1].InTransaction);
        Assert.Equal(new long[] { 1, 2, 3, 61 }, d.Ids("app"));                  // applied at once; 60 never
        Assert.Equal(1, srv.Connections);
    }

    // ---- when it cannot be put back -------------------------------------------------------

    [Theory]
    [InlineData("SET CONSISTENCY DEFAULT")]                                      // a server before 0.349 does not know it
    [InlineData("SET SCAN BUDGET DEFAULT")]
    [InlineData("USE \"app\"")]
    [InlineData("ROLLBACK")]
    public void A_connection_that_cannot_be_put_back_is_closed_and_the_next_borrower_gets_a_new_one(string refused)
    {
        var d = new Sessions { Old = refused == "SET CONSISTENCY DEFAULT" };
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, "app");
        pool.WithConnection(c => Rows(c));
        var first = srv.Accepted.Single();
        if (!d.Old) d.Refuse = (sql, conn) => conn == first && sql == refused ? "refused: " + sql : null;
        pool.WithConnection(c =>
        {
            Exec(c, "BEGIN");
            Exec(c, "INSERT INTO notes (id, body) VALUES (70, 'never committed')");
            Exec(c, "USE other");
            Exec(c, "SET SCAN BUDGET ROWS 1");
            Exec(c, "SET CONSISTENCY ALL");
            return 0;
        });
        // Not kept: its session is still, in part, the last borrower's.
        Assert.Equal(refused, d.Log[^1].Sql);
        Assert.Equal(0, pool.Members().Sum(m => m.Idle + m.InUse));
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (srv.Live != 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "the connection that could not be put back is still open");
            System.Threading.Thread.Sleep(10);
        }

        int logged = d.Log.Length;
        pool.WithConnection(c =>
        {
            Assert.Equal(3L, Rows(c));
            Assert.Equal(new long[] { 1, 2, 3 }, Ids(c));
            return 0;
        });
        Assert.Equal(2, srv.Connections);
        Assert.Equal(new[] { Use("app"), Count, Scan }, d.Since(logged));        // a new connection, and nothing of the old session
        Assert.Equal(new Sessions.Ran(Scan, "app", "QUORUM", null, false, srv.Accepted[1]), d.Log[^1]);
        Assert.Equal(new long[] { 1, 2, 3 }, d.Ids("app"));                      // row 70 went with its connection
    }

    // ---- a re-dial afterwards ---------------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("app")]
    public void A_re_dial_for_the_next_borrower_restores_the_pool_s_session_not_the_last_borrower_s(string database)
    {
        string home = database.Length > 0 ? database : "default";
        var d = new Sessions();
        using var srv = new FakeServer(d.Handle);
        using var pool = Pool(srv, database);
        pool.WithConnection(c =>
        {
            Exec(c, "USE other");
            Exec(c, "SET SCAN BUDGET ROWS 1");
            Exec(c, "SET CONSISTENCY ALL");
            return 0;
        });
        // The next borrower holds the connection when the server restarts.
        var c2 = pool.Acquire();
        int logged = d.Log.Length;
        srv.CloseAll(seenBy: c2);
        Assert.Equal(d.Rows(home), Rows(c2));
        Assert.Equal(database.Length > 0 ? new[] { Use(database), Count } : new[] { Count }, d.Since(logged));
        Assert.Equal(new Sessions.Ran(Count, home, "QUORUM", null, false, srv.Accepted[1]), d.Log[^1]);
        pool.Release(c2);
        Assert.Equal(2, srv.Connections);
    }
}
