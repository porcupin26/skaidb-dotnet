// The session database across a re-dial and across pool borrowers, against
// an in-process fake server that keeps a current database per connection —
// asserted by the database each statement actually ran in.

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Xunit;

namespace Skaidb.Tests;

/// <summary>
/// A fake server with databases. Every connection starts in <c>default</c>;
/// <c>USE [DATABASE] name</c> moves it (an Error for a database that is not
/// in <see cref="Notes"/>, or that is <see cref="Dropped"/>). <c>--</c>
/// comments are skipped as a real server skips them: to the end of the
/// line, unless the dashes are inside a quoted name or string. Each database
/// holds a table <c>notes</c>: <c>SELECT count(*) FROM notes</c> answers the
/// row count of the connection's current database and <c>INSERT INTO notes</c>
/// adds a row to it. Everything else is answered Ddl. <see cref="Log"/> has
/// every statement with the database it ran in and the socket it arrived on.
/// </summary>
public sealed class Databases
{
    private static readonly Regex Use = new(
        @"^\s*USE\s+(?:DATABASE\s+)?(?:""((?:[^""]|"""")+)""|(\w+))\s*;?\s*$", RegexOptions.IgnoreCase);

    private readonly object _gate = new();
    private readonly Dictionary<FakeConnection, string> _current = new();
    private readonly List<(string Sql, string Database, FakeConnection Socket)> _log = new();

    /// <summary>Rows of <c>notes</c> per database.</summary>
    public readonly Dictionary<string, int> Notes = new()
    {
        ["default"] = 0, ["app"] = 3, ["other"] = 7, ["App"] = 11, ["we\"ird db"] = 5, ["DATABASE"] = 13, ["a--b"] = 9,
    };

    /// <summary>A database that no longer exists: USE of it is refused.</summary>
    public volatile string? Dropped;

    public (string Sql, string Database, FakeConnection Socket)[] Log { get { lock (_gate) return _log.ToArray(); } }

    public int Rows(string database) { lock (_gate) return Notes[database]; }

    private static string SkipComments(string sql)
    {
        var kept = new System.Text.StringBuilder();
        bool inName = false, inString = false;
        foreach (string line in sql.Split('\n'))
        {
            int end = line.Length;
            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == '"' && !inString) inName = !inName;
                else if (line[i] == '\'' && !inName) inString = !inString;
                else if (!inName && !inString && line[i] == '-' && i + 1 < line.Length && line[i + 1] == '-')
                {
                    end = i;
                    break;
                }
            }
            kept.Append(line, 0, end).Append('\n');
        }
        return kept.ToString();
    }

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        if (req.Op == 2) return new[] { F.Error("statement kind cannot be prepared") };
        if (req.Op != 1 && req.Op != 5) return null;   // Hello -> Ddl
        lock (_gate)
        {
            string db = _current.GetValueOrDefault(conn, "default");
            _log.Add((req.Sql, db, conn));
            var use = Use.Match(SkipComments(req.Sql));
            if (use.Success)
            {
                string name = use.Groups[1].Success ? use.Groups[1].Value.Replace("\"\"", "\"") : use.Groups[2].Value;
                if (!Notes.ContainsKey(name) || name == Dropped)
                    return new[] { F.Error($"database \"{name}\" does not exist") };
                _current[conn] = name;
                return new[] { F.Ddl() };
            }
            if (req.Sql.StartsWith("SELECT count(*) FROM notes", StringComparison.Ordinal))
                return new[] { F.Rows(new[] { "count" }, new object?[] { (long)Notes[db] }) };
            if (req.Sql.StartsWith("INSERT INTO notes", StringComparison.Ordinal))
            {
                Notes[db]++;
                return new[] { F.Mutation(1) };
            }
            return new[] { F.Ddl() };
        }
    }
}

public class SessionDatabaseTests
{
    private const string Count = "SELECT count(*) FROM notes";
    private const string Insert = "INSERT INTO notes (id, body) VALUES (100, 'written after the re-dial')";

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

    /// <summary>The Async methods complete synchronously (the driver's I/O is synchronous).</summary>
    private static void Wait(System.Threading.Tasks.Task<int> done)
    {
        done.GetAwaiter().GetResult();
    }

    /// <summary>
    /// The server closes every connection while the client sits idle, as a
    /// restart does; returns once <paramref name="c"/>'s socket has seen it.
    /// </summary>
    private static void LoseWhileIdle(FakeServer srv, SkaidbConnection c)
    {
        srv.CloseAll();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (c.IsUsable)
        {
            Assert.True(DateTime.UtcNow < deadline, "the close never reached the client");
            Thread.Sleep(10);
        }
    }

    /// <summary>
    /// The scenario: the session is in <paramref name="database"/>, the
    /// connection is lost while idle, then a SELECT and an INSERT. Both ran
    /// in that database, on a new connection whose first statement was the
    /// USE.
    /// </summary>
    private static void BothStatementsRunInItAfterARedial(FakeServer srv, Databases d, SkaidbConnection c, string database)
    {
        int rows = d.Rows(database);
        Assert.Equal(rows, Rows(c));
        int dialed = srv.Connections;
        int logged = d.Log.Length;
        LoseWhileIdle(srv, c);
        Assert.Equal(rows, Rows(c));                                             // not the 0 rows of default's notes
        Assert.Equal(1, Exec(c, Insert));
        Assert.Equal(rows + 1, d.Rows(database));
        Assert.Equal(0, d.Rows("default"));                                      // nothing was written there
        Assert.Equal(dialed + 1, srv.Connections);
        var since = d.Log.Skip(logged).ToArray();
        Assert.Equal(new[] { "USE \"" + database.Replace("\"", "\"\"") + "\"", Count, Insert }, since.Select(l => l.Sql));
        Assert.Single(since.Select(l => l.Socket).Distinct());                   // the USE went first on the new connection
        Assert.Equal(new[] { "default", database, database }, since.Select(l => l.Database));
        Assert.Equal(database, c.Database);
    }

    // ---- the Database option and ChangeDatabase() ------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void The_database_of_the_connection_string_or_of_ChangeDatabase_is_entered_again_after_a_re_dial(bool changed)
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, changed ? "" : "Database=app"));
        c.Open();
        if (changed) c.ChangeDatabase("app");
        BothStatementsRunInItAfterARedial(srv, d, c, "app");
    }

    [Fact]
    public void A_pooled_connection_that_re_dials_enters_the_pool_s_database_again()
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv, "Database=app"));
        var c = pool.Acquire();
        BothStatementsRunInItAfterARedial(srv, d, c, "app");                     // re-dialed while checked out
        pool.Release(c);
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
        // Lost while idle in the pool: the checkout dials, and that connection enters it too.
        LoseWhileIdle(srv, c);
        Assert.Equal(4L, pool.WithConnection(Rows));
        Assert.Equal(new[] { "USE \"app\"", Count }, d.Log.TakeLast(2).Select(l => l.Sql));
        Assert.Equal("app", d.Log[^1].Database);
    }

    [Fact]
    public void When_the_database_of_the_connection_string_cannot_be_entered_again_the_statement_fails_with_that_error()
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, "Database=app"));
        c.Open();
        NothingRunsElsewhereWhileItCannotBeEntered(srv, d, c);
    }

    /// <summary>
    /// The session is in <c>app</c>; the connection is lost and <c>app</c> is
    /// gone when the driver re-dials. Every statement fails with the USE's
    /// error and none reaches the server, until the database is back.
    /// </summary>
    private static void NothingRunsElsewhereWhileItCannotBeEntered(FakeServer srv, Databases d, SkaidbConnection c)
    {
        Assert.Equal(3L, Rows(c));
        int logged = d.Log.Length;
        d.Dropped = "app";
        LoseWhileIdle(srv, c);
        Assert.Equal("database \"app\" does not exist", Assert.Throws<SkaidbException>(() => Rows(c)).Message);
        Assert.Equal("database \"app\" does not exist", Assert.Throws<SkaidbException>(() => Exec(c, Insert)).Message);
        Assert.False(c.IsUsable);
        // Two dials, each stopped at its USE: neither statement was sent, in default or anywhere.
        Assert.Equal(new[] { "USE \"app\"", "USE \"app\"" }, d.Log.Skip(logged).Select(l => l.Sql));
        Assert.Equal(0, d.Rows("default"));
        Assert.Equal(3, d.Rows("app"));
        // The database is back: the next statement enters it and runs.
        d.Dropped = null;
        Assert.Equal(3L, Rows(c));
        Assert.Equal("app", d.Log[^1].Database);
        Assert.True(c.IsUsable);
    }

    // ---- a USE sent as a statement ----------------------------------------------------

    [Theory]
    [InlineData("USE app", "app")]
    [InlineData("use app", "app")]
    [InlineData("USE DATABASE app", "app")]
    [InlineData("Use Database \"app\"", "app")]
    [InlineData("  USE\tapp ;\n", "app")]
    [InlineData("USE \"app\";", "app")]
    [InlineData("USE App", "App")]                                               // the name as written: another database than app
    [InlineData("USE \"we\"\"ird db\"", "we\"ird db")]
    [InlineData("USE app -- the app database", "app")]                           // a comment the server skips
    [InlineData("-- into app\nUSE app", "app")]
    [InlineData("-- first\nUSE app;  -- then\n", "app")]
    [InlineData("USE -- which one\napp", "app")]
    [InlineData("USE \"a--b\" -- named with dashes", "a--b")]                    // dashes inside the quotes are the name
    [InlineData("USE DATABASE", "DATABASE")]                                     // the database called so, as the server reads it
    [InlineData("USE DATABASE DATABASE", "DATABASE")]
    public void A_database_entered_with_a_USE_statement_is_entered_again_after_a_re_dial(string use, string database)
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv));
        c.Open();
        Assert.Equal(0, Exec(c, use));
        BothStatementsRunInItAfterARedial(srv, d, c, database);
    }

    [Theory]
    [InlineData("ExecuteScalar")]
    [InlineData("ExecuteReader")]
    [InlineData("ExecuteNonQueryAsync")]
    [InlineData("DbBatch")]
    [InlineData("Stream")]
    public void A_USE_is_noted_whichever_way_the_statement_is_run(string way)
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, "Database=other"));           // and it replaces the connection string's
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "USE app";
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
                    batch.BatchCommands.Add("USE app");
                    batch.BatchCommands.Add(Count);
                    Assert.Equal(3L, batch.ExecuteScalar());
                }
                break;
            default:
                Assert.Empty(c.Stream("USE app").ToList());
                break;
        }
        BothStatementsRunInItAfterARedial(srv, d, c, "app");
    }

    [Fact]
    public void The_last_database_entered_is_the_one_entered_again_and_a_refused_USE_changes_nothing()
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, "Database=other"));
        c.Open();
        Exec(c, "USE app");
        c.ChangeDatabase("other");
        Exec(c, "USE app");
        // The server said no: the session is where it was, and so is the driver.
        Assert.Equal("database \"nope\" does not exist", Assert.Throws<SkaidbException>(() => Exec(c, "USE nope")).Message);
        Assert.Equal("database \"nope\" does not exist", Assert.Throws<SkaidbException>(() => c.ChangeDatabase("nope")).Message);
        BothStatementsRunInItAfterARedial(srv, d, c, "app");
    }

    [Theory]
    [InlineData("USE app; SELECT 1")]                                            // two statements
    [InlineData("USE app extra")]
    [InlineData("USE a.b")]
    [InlineData("USE 'app'")]
    [InlineData("USE \"app")]                                                    // the quote never closes
    [InlineData("USE \"\"")]                                                     // no name in it
    [InlineData("USE")]
    [InlineData("USE -- nothing")]                                               // the name is in the comment
    [InlineData("-- USE app")]                                                   // all of it is
    [InlineData("SELECT 1 -- USE app")]
    [InlineData("USE \"a--b")]                                                   // not a comment: an open quote
    [InlineData("USE DATABASE \"app")]
    [InlineData("EXPLAIN USE app")]
    [InlineData("SELECT 'USE app'")]
    [InlineData("USEFUL app")]
    public void A_statement_that_is_not_exactly_one_USE_is_left_alone(string sql)
    {
        // The fake answers each of these with success, and still the driver
        // takes nothing from a text it cannot read as one USE.
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, "Database=other"));
        c.Open();
        Exec(c, sql);
        Assert.Equal("other", c.Database);
        LoseWhileIdle(srv, c);
        Assert.Equal(7L, Rows(c));
        Assert.Equal(new[] { "USE \"other\"", Count }, d.Log.TakeLast(2).Select(l => l.Sql));
    }

    [Fact]
    public void When_a_database_entered_with_USE_cannot_be_entered_again_the_statement_fails_with_that_error()
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv));
        c.Open();
        Exec(c, "USE app");
        NothingRunsElsewhereWhileItCannotBeEntered(srv, d, c);
    }

    // ---- a pool and its borrowers -----------------------------------------------------

    [Fact]
    public void A_pooled_connection_that_re_dials_enters_the_database_its_borrower_entered_with_USE()
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv, "Database=app"));
        var c = pool.Acquire();
        Exec(c, "USE other");
        BothStatementsRunInItAfterARedial(srv, d, c, "other");                   // not back in the pool's app, mid-checkout
        pool.Release(c);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_borrower_s_USE_does_not_reach_the_next_borrower_of_a_pool_with_a_database(bool changed)
    {
        var d = new Databases();
        using var srv = new FakeServer(d.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv, "Database=app"));
        pool.WithConnection(c =>
        {
            if (changed) c.ChangeDatabase("other");
            else Exec(c, "USE other");
            return Rows(c);
        });
        // The next borrower gets the same connection, in the pool's database.
        Assert.Equal(3L, pool.WithConnection(Rows));
        Assert.Equal("app", d.Log[^1].Database);
        Assert.Equal(1, srv.Connections);                                        // put back, not thrown away
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
        Assert.Single(d.Log.Select(l => l.Socket).Distinct());
        Assert.Equal("USE \"app\"", d.Log[^2].Sql);                              // sent when the connection was handed back

        // A connection that cannot be put back is not kept: the pool's
        // database is gone, and no later borrower runs anything in other.
        d.Dropped = "app";
        pool.WithConnection(c => Exec(c, "USE other"));
        Assert.Equal(0, pool.Members().Sum(m => m.Idle + m.InUse));
        Assert.Equal("database \"app\" does not exist", Assert.Throws<SkaidbException>(() => pool.WithConnection(Rows)).Message);
        Assert.NotEqual(Count, d.Log[^1].Sql);
    }

    // ---- Open() --------------------------------------------------------------------------

    [Fact]
    public void An_Open_that_fails_at_its_USE_leaves_the_connection_closed()
    {
        var d = new Databases { Dropped = "app" };
        using var srv = new FakeServer(d.Handle);
        using var c = new SkaidbConnection(Cs(srv, "Database=app"));
        var states = new List<(ConnectionState, ConnectionState)>();
        c.StateChange += (_, e) => states.Add((e.OriginalState, e.CurrentState));
        Assert.Equal("database \"app\" does not exist", Assert.Throws<SkaidbException>(() => c.Open()).Message);
        Assert.False(c.IsOpen);
        Assert.Equal(ConnectionState.Closed, c.State);
        Assert.Empty(states);
        Assert.Equal("connection is not open", Assert.Throws<SkaidbException>(() => Rows(c)).Message);
        Assert.Equal(1, srv.Connections);
        // Closed, so Open() dials again: it is not taken for already open.
        d.Dropped = null;
        c.Open();
        Assert.Equal(2, srv.Connections);
        Assert.Equal(3L, Rows(c));
        Assert.Equal(new[] { (ConnectionState.Closed, ConnectionState.Open) }, states);
    }
}
