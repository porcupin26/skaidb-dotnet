// The connection's prepared-statement cache against a fake server that keeps
// a statement table per connection as a real one does: 256 slots, freed only
// by OP_CLOSE or with the socket.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Skaidb.Tests;

/// <summary>
/// A fake server with the prepared-statement table of a real one: at most
/// <see cref="Max"/> statements open per connection; a prepare beyond that
/// is refused with the server's message, and OP_CLOSE frees a slot. An id
/// is an index into the table of the connection that prepared it: every
/// connection starts at 1 and a freed id is the next one given out, so an
/// id carried from one connection to another, or kept past its close,
/// names another statement there. Executions are recorded with the text
/// the id names ON THE CONNECTION THAT RECEIVED IT and the typed
/// parameters as they arrived; a statement that arrives as text (OP_QUERY)
/// is recorded apart, which is where a fallback to client-side literals
/// would show.
/// </summary>
public sealed class StatementTable
{
    public const int Max = 256;
    public const string TooMany = "too many prepared statements (max 256); close some first";

    private readonly object _gate = new();
    private readonly Dictionary<FakeConnection, Dictionary<uint, string>> _open = new();

    public readonly List<(string Sql, List<object?> Params)> Executed = new();
    public readonly List<string> Prepared = new();
    /// <summary>The id each prepare was answered with, alongside <see cref="Prepared"/>.</summary>
    public readonly List<uint> Ids = new();
    public readonly List<string> Closed = new();
    public readonly List<string> AsText = new();
    public int Refused;
    public int MostOpen;
    /// <summary>Refuse every prepare, as a full table does.</summary>
    public volatile bool Full;

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        lock (_gate)
        {
            if (!_open.TryGetValue(conn, out var table)) _open[conn] = table = new Dictionary<uint, string>();
            switch (req.Op)
            {
                case 1:
                    AsText.Add(req.Sql);
                    return new[] { F.Mutation(1) };
                case 2:
                {
                    if (Full || table.Count >= Max)
                    {
                        Refused++;
                        return new[] { F.Error(TooMany) };
                    }
                    uint id = 1;
                    while (table.ContainsKey(id)) id++;
                    table[id] = req.Sql;
                    Prepared.Add(req.Sql);
                    Ids.Add(id);
                    MostOpen = Math.Max(MostOpen, table.Count);
                    return new[] { F.Prepared(id, (ushort)req.Sql.Count(ch => ch == '?')) };
                }
                case 3:
                {
                    if (!table.TryGetValue(req.Id, out var sql)) return new[] { F.Error($"unknown prepared statement {req.Id}") };
                    Executed.Add((sql, req.Params));
                    return new[] { F.Mutation(1) };
                }
                case 4:
                {
                    if (!table.Remove(req.Id, out var sql)) return new[] { F.Error($"unknown prepared statement {req.Id}") };
                    Closed.Add(sql);
                    return new[] { F.Ddl() };
                }
                case 7:
                {
                    if (!table.TryGetValue(req.Id, out var sql)) return new[] { F.Error($"unknown prepared statement {req.Id}") };
                    foreach (var row in req.Rows) Executed.Add((sql, row));
                    return new[] { F.Mutation((ulong)req.Rows.Count) };
                }
                default:
                    return null;   // Hello -> Ddl
            }
        }
    }
}

public class PreparedStatementTests
{
    private static readonly DateTimeOffset At = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
    private static readonly byte[] Raw = { 0x00, 0x01, 0xfe, 0xff };

    private static SkaidbConnection Connect(FakeServer srv)
    {
        var c = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5");
        c.Open();
        return c;
    }

    /// <summary>The nth of the distinct statements: another column each time.</summary>
    private static string Insert(int n) => $"INSERT INTO t (id, a{n}, meta, at, raw) VALUES (?, ?, ?, ?, ?)";

    private static object?[] Values(int n) => new object?[]
    {
        (long)n,
        new object?[] { "tag", (long)n },                                        // an array: no SQL literal form
        new Dictionary<string, object?> { ["n"] = (long)n },                     // a document: none either
        At,
        Raw,
    };

    [Fact]
    public void A_prepare_refused_for_a_full_table_is_the_statement_s_error_and_nothing_is_sent_as_literals()
    {
        var t = new StatementTable { Full = true };
        using var srv = new FakeServer(t.Handle);
        using var c = Connect(srv);
        // Scalars only: these are values the text binder COULD render.
        const string sql = "INSERT INTO t (id, name, at, raw) VALUES (?, ?, ?, ?)";
        var values = new object?[] { 1L, "Ada", At, Raw };
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values) cmd.Parameters.Add(v);
        Assert.Equal(StatementTable.TooMany, Assert.Throws<SkaidbException>(() => cmd.ExecuteNonQuery()).Message);
        Assert.Equal(StatementTable.TooMany, Assert.Throws<SkaidbException>(() => cmd.ExecuteScalar()).Message);
        Assert.Equal(StatementTable.TooMany, Assert.Throws<SkaidbException>(() => cmd.Prepare()).Message);
        Assert.Equal(StatementTable.TooMany, Assert.Throws<SkaidbException>(() =>
            cmd.ExecuteBatch(new List<IReadOnlyList<object?>> { values, values })).Message);
        Assert.Equal(StatementTable.TooMany, Assert.Throws<SkaidbException>(() =>
        {
            using var batch = c.CreateBatch();
            for (int i = 0; i < 2; i++)
            {
                var one = batch.BatchCommands.Add(sql);
                foreach (var v in values) one.Parameters.Add(v);
            }
            batch.ExecuteNonQuery();
        }).Message);
        Assert.Equal(5, t.Refused);
        Assert.Empty(t.AsText);                                                  // never as text with client-side literals
        Assert.Empty(t.Executed);
        Assert.True(c.IsUsable);                                                 // a statement error: the connection stays
        Assert.Equal(1, srv.Connections);
    }

    [Fact]
    public void A_prepared_statement_belongs_to_the_connection_not_to_the_command_that_prepared_it()
    {
        var t = new StatementTable();
        using var srv = new FakeServer(t.Handle);
        using var c = Connect(srv);
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = Insert(1);
            foreach (var v in Values(1)) cmd.Parameters.Add(v);
            cmd.Prepare();
            cmd.Prepare();                                                       // already prepared: nothing is sent
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        // Disposing the command closes nothing: the next command with the
        // same text runs the same server statement.
        Assert.Empty(t.Closed);
        using (var again = c.CreateCommand())
        {
            again.CommandText = Insert(1);
            foreach (var v in Values(1)) again.Parameters.Add(v);
            Assert.Equal(1, again.ExecuteNonQuery());
        }
        Assert.Single(t.Prepared);
        Assert.Equal(2, t.Executed.Count);
        Assert.Empty(t.Closed);
    }

    // ---- an id belongs to the connection that issued it ------------------------------

    private const string Update = "UPDATE t SET v = ? WHERE id = ?";
    private const string Delete = "DELETE FROM t WHERE v = ? AND id = ?";             // as many parameters, another effect

    private static SkaidbCommand Command(SkaidbConnection c, string sql)
    {
        var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(5L);
        cmd.Parameters.Add(1L);
        return cmd;
    }

    private static SkaidbBatch Batch(SkaidbConnection c, string sql)
    {
        // Two commands with one text: a run the batch sends as ONE OP_EXECUTE_BATCH of a prepared statement.
        var batch = c.CreateBatch();
        for (long row = 1; row <= 2; row++)
        {
            var one = batch.BatchCommands.Add(sql);
            one.Parameters.Add(5L);
            one.Parameters.Add(row);
        }
        return batch;
    }

    private static void LoseWhileIdle(FakeServer srv, SkaidbConnection c)
    {
        srv.CloseAll();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (c.IsUsable)
        {
            Assert.True(DateTime.UtcNow < deadline, "the close never reached the client");
            System.Threading.Thread.Sleep(10);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_statement_prepared_before_a_connection_loss_is_prepared_again_and_never_runs_as_another(bool batched)
    {
        var t = new StatementTable();
        using var srv = new FakeServer(t.Handle);
        using var c = Connect(srv);
        using var update = Command(c, Update);
        using var delete = Command(c, Delete);
        using var updates = Batch(c, Update);
        using var deletes = Batch(c, Delete);
        void RunUpdate() { if (batched) updates.ExecuteNonQuery(); else update.ExecuteNonQuery(); }
        void RunDelete() { if (batched) deletes.ExecuteNonQuery(); else delete.ExecuteNonQuery(); }
        int each = batched ? 2 : 1;

        // Prepared on the first connection: the UPDATE takes id 1, the DELETE id 2.
        if (batched) { RunUpdate(); RunDelete(); }
        else { update.Prepare(); delete.Prepare(); }
        Assert.Equal(new[] { Update, Delete }, t.Prepared);
        Assert.Equal(new uint[] { 1, 2 }, t.Ids);

        LoseWhileIdle(srv, c);

        // On the new connection the DELETE comes first and takes id 1: the
        // id the UPDATE had on the old one. Then the UPDATE, through the
        // command (or batch) that was prepared before the loss.
        int before = t.Executed.Count;
        RunDelete();
        RunUpdate();
        Assert.Equal(new[] { Update, Delete, Delete, Update }, t.Prepared);        // each prepared again, on its new connection
        Assert.Equal(new uint[] { 1, 2, 1, 2 }, t.Ids);
        Assert.Equal(Enumerable.Repeat(Delete, each).Concat(Enumerable.Repeat(Update, each)),
                     t.Executed.Skip(before).Select(e => e.Sql));                  // and each ran as itself
        Assert.Empty(t.AsText);
        Assert.Equal(2, srv.Connections);
        // Again, now both known on this connection: no further prepare.
        RunUpdate();
        RunDelete();
        Assert.Equal(4, t.Prepared.Count);
        Assert.Equal(Enumerable.Repeat(Update, each).Concat(Enumerable.Repeat(Delete, each)),
                     t.Executed.TakeLast(2 * each).Select(e => e.Sql));
    }

    [Fact]
    public void A_prepared_command_moved_to_another_connection_is_prepared_there()
    {
        var t = new StatementTable();
        using var srv = new FakeServer(t.Handle);
        using var first = Connect(srv);
        using var second = Connect(srv);
        using var update = Command(first, Update);
        update.Prepare();                                                        // id 1 on the first connection
        using var delete = Command(second, Delete);
        delete.Prepare();                                                        // id 1 on the second
        Assert.Equal(new uint[] { 1, 1 }, t.Ids);

        update.Connection = second;
        Assert.Equal(1, update.ExecuteNonQuery());
        Assert.Equal(Update, t.Executed[^1].Sql);                                // not the DELETE that id 1 names there
        Assert.Equal(new[] { Update, Delete, Update }, t.Prepared);
        Assert.Equal(new uint[] { 1, 1, 2 }, t.Ids);

        // The same between the connections of a pool.
        using var pool = new SkaidbConnectionPool($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret;Timeout=5");
        var x = pool.Acquire();
        var y = pool.Acquire();
        using var onX = Command(x, Update);
        onX.Prepare();
        using var onY = Command(y, Delete);
        onY.Prepare();
        onX.Connection = y;
        Assert.Equal(1, onX.ExecuteNonQuery());
        Assert.Equal(Update, t.Executed[^1].Sql);
        using var moved = Batch(x, Delete);
        moved.Connection = y;
        Assert.Equal(2, moved.ExecuteNonQuery());
        Assert.Equal(new[] { Delete, Delete }, t.Executed.TakeLast(2).Select(e => e.Sql));
        pool.Release(x);
        pool.Release(y);
        Assert.Empty(t.AsText);
        Assert.Equal(0, t.Refused);
    }
}
