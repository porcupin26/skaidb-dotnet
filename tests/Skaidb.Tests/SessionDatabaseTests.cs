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
/// in <see cref="Notes"/>, or that is <see cref="Dropped"/>). Each database
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
        ["default"] = 0, ["app"] = 3, ["other"] = 7, ["App"] = 11, ["we\"ird db"] = 5, ["DATABASE"] = 13,
    };

    /// <summary>A database that no longer exists: USE of it is refused.</summary>
    public volatile string? Dropped;

    public (string Sql, string Database, FakeConnection Socket)[] Log { get { lock (_gate) return _log.ToArray(); } }

    public int Rows(string database) { lock (_gate) return Notes[database]; }

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        if (req.Op == 2) return new[] { F.Error("statement kind cannot be prepared") };
        if (req.Op != 1 && req.Op != 5) return null;   // Hello -> Ddl
        lock (_gate)
        {
            string db = _current.GetValueOrDefault(conn, "default");
            _log.Add((req.Sql, db, conn));
            var use = Use.Match(req.Sql);
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
}
