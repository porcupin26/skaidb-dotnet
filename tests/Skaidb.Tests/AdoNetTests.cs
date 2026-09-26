// The ADO.NET surface: provider factory registration, the connection string
// builder, DbCommand/DbParameter placeholders, the DbDataReader, DbBatch,
// error mapping, and Dapper on top, all against the in-process fake server.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Dapper;
using Xunit;

namespace Skaidb.Tests;

public class AdoNetTests
{
    private static string Cs(FakeServer srv, string extra = "") =>
        $"Host=127.0.0.1;Port={srv.Port};Username=ada;Password=secret;Timeout=5;{extra}";

    private static SkaidbConnection Connect(FakeServer srv, string extra = "")
    {
        var c = new SkaidbConnection(Cs(srv, extra));
        c.Open();
        return c;
    }

    // ---- factory ----------------------------------------------------------------

    [Fact]
    public void Factory_registers_and_resolves_through_DbProviderFactories()
    {
        DbProviderFactories.RegisterFactory("Skaidb", SkaidbFactory.Instance);
        Assert.Same(SkaidbFactory.Instance, DbProviderFactories.GetFactory("Skaidb"));
        Assert.Equal("Skaidb", SkaidbFactory.ProviderInvariantName);
        Assert.Contains("Skaidb", DbProviderFactories.GetProviderInvariantNames());

        // By type: DbProviderFactories finds the public static Instance field.
        DbProviderFactories.RegisterFactory("Skaidb.ByType", typeof(SkaidbFactory));
        Assert.Same(SkaidbFactory.Instance, DbProviderFactories.GetFactory("Skaidb.ByType"));

        SkaidbFactory.Register();                                            // idempotent helper
        Assert.Same(SkaidbFactory.Instance, DbProviderFactories.GetFactory(SkaidbFactory.ProviderInvariantName));

        var f = SkaidbFactory.Instance;
        Assert.IsType<SkaidbConnection>(f.CreateConnection());
        Assert.IsType<SkaidbCommand>(f.CreateCommand());
        Assert.IsType<SkaidbParameter>(f.CreateParameter());
        Assert.IsType<SkaidbConnectionStringBuilder>(f.CreateConnectionStringBuilder());
        Assert.True(f.CanCreateBatch);
        Assert.IsType<SkaidbBatch>(f.CreateBatch());
        Assert.IsType<SkaidbBatchCommand>(f.CreateBatchCommand());
        using var conn = new SkaidbConnection();
        Assert.Same(SkaidbFactory.Instance, DbProviderFactories.GetFactory(conn));
    }

    [Fact]
    public void Provider_agnostic_code_runs_through_the_factory()
    {
        const string sql = "SELECT name FROM t WHERE id = ?";
        var s = new Scripted().Prepare(sql, 1);
        s.OnExecute = (_, req) => new[] { F.Rows(new[] { "name" }, new object?[] { "id=" + req.Params[0] }) };
        using var srv = new FakeServer(s.Handle);
        DbProviderFactories.RegisterFactory("Skaidb", SkaidbFactory.Instance);

        DbProviderFactory factory = DbProviderFactories.GetFactory("Skaidb");
        using DbConnection conn = factory.CreateConnection()!;
        conn.ConnectionString = Cs(srv);
        Assert.Equal(ConnectionState.Closed, conn.State);
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
        using DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM t WHERE id = @id";
        DbParameter p = cmd.CreateParameter();
        p.ParameterName = "@id";
        p.Value = 7L;
        cmd.Parameters.Add(p);
        Assert.Equal("id=7", cmd.ExecuteScalar());
        using DbDataReader r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal("id=7", r.GetFieldValue<string>(0));
        conn.Close();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    // ---- connection strings -------------------------------------------------------

    [Fact]
    public void Connection_string_builder_parses_host_lists_aliases_and_quoting()
    {
        var b = new SkaidbConnectionStringBuilder(
            "server=a, b:7001 ,[::1]:7002,::2;PORT=7005;User Id=ada;Pwd='p;w=d';db=app;Consistency=all;Tls=true;Connect Timeout=2.5");
        Assert.Equal("a, b:7001 ,[::1]:7002,::2", b.Host);
        Assert.Equal(7005, b.Port);
        Assert.Equal("ada", b.Username);
        Assert.Equal("p;w=d", b.Password);
        Assert.Equal("app", b.Database);
        Assert.Equal(SkaidbConsistency.All, b.Consistency);
        Assert.True(b.Tls);
        Assert.Equal(TimeSpan.FromSeconds(2.5), b.Timeout);
        Assert.True(b.ContainsKey("user"));                                    // aliases resolve
        Assert.Equal("ada", b["UID"]);
        // Canonical names on the way out, and the string round-trips.
        Assert.Contains("Username=ada", b.ConnectionString);
        Assert.Equal(b.ConnectionString, new SkaidbConnectionStringBuilder(b.ConnectionString).ConnectionString);

        using var c = new SkaidbConnection(b.ConnectionString);
        Assert.Equal(new[] { "a:7005", "b:7001", "[::1]:7002", "[::2]:7005" }, c.Seeds);
        Assert.Equal("a", c.Host);
        Assert.Equal(7005, c.Port);
        Assert.Equal("app", c.Database);
        Assert.Equal("a:7005,b:7001,[::1]:7002,[::2]:7005", c.DataSource);
        Assert.Equal(2, c.ConnectionTimeout);
        Assert.Equal(("::1", 7002), SkaidbConnection.SplitEndpoint("[::1]:7002", 1));
        Assert.Equal(("h", 9), SkaidbConnection.SplitEndpoint("h:9", 1));
        Assert.Equal(("h", 1), SkaidbConnection.SplitEndpoint("h", 1));
    }

    [Fact]
    public void Connection_string_builder_validates_and_builds()
    {
        var b = new SkaidbConnectionStringBuilder
        {
            Host = "db1,db2",
            Port = 7100,
            Username = "u",
            Password = "a;b",
            Database = "d",
            Consistency = SkaidbConsistency.One,
            TlsCa = "/ca.pem",
            TlsClientCert = "/c.pem",
            TlsClientKey = "/k.pem",
            AuthMechanism = SkaidbAuthMechanism.Certificate,
        };
        using var c = new SkaidbConnection(b.ConnectionString);
        Assert.Equal(new[] { "db1:7100", "db2:7100" }, c.Seeds);
        Assert.Equal(SkaidbConsistency.One, c.Consistency);
        Assert.Equal(SkaidbAuthMechanism.Certificate, c.AuthMechanism);
        Assert.True(c.Tls);                                                     // implied
        Assert.Equal("a;b", new SkaidbConnectionStringBuilder(b.ConnectionString).Password);

        Assert.Throws<ArgumentException>(() => new SkaidbConnectionStringBuilder("Nope=1"));
        Assert.Throws<ArgumentException>(() => new SkaidbConnectionStringBuilder("Port=x"));
        Assert.Throws<ArgumentException>(() => new SkaidbConnectionStringBuilder("AuthMechanism=kerberos"));
        Assert.Throws<ArgumentException>(() => b["Consistency"] = "maybe");
        // The connection reports the same problems as its own exception type.
        Assert.Throws<SkaidbException>(() => new SkaidbConnection("Port=0"));
        Assert.Throws<SkaidbException>(() => new SkaidbConnection("Timeout=-1"));
        Assert.Equal(SkaidbAuthMechanism.Certificate, new SkaidbConnectionStringBuilder("auth mechanism=external").AuthMechanism);
        Assert.Equal(SkaidbAuthMechanism.Scram, new SkaidbConnectionStringBuilder("").AuthMechanism);
    }

    [Fact]
    public void Connection_string_changes_only_while_closed_and_Close_reopens()
    {
        using var srv = new FakeServer();
        using var c = new SkaidbConnection();
        var states = new List<(ConnectionState, ConnectionState)>();
        c.StateChange += (_, e) => states.Add((e.OriginalState, e.CurrentState));
        c.ConnectionString = Cs(srv);
        Assert.Equal("ada", c.User);
        c.Open();
        Assert.Throws<InvalidOperationException>(() => c.ConnectionString = "Host=elsewhere");
        c.Close();
        c.ConnectionString = Cs(srv, "Database=app");
        Assert.Equal("app", c.Database);
        Assert.Equal(new[] { (ConnectionState.Closed, ConnectionState.Open), (ConnectionState.Open, ConnectionState.Closed) }, states);
    }

    // ---- transactions + errors ----------------------------------------------------

    [Fact]
    public void Transactions_are_not_supported_and_errors_are_DbExceptions()
    {
        var s = new Scripted().Answer("SELECT nope", F.Error("unknown column: nope"));
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Throws<NotSupportedException>(() => c.BeginTransaction());
        Assert.Throws<NotSupportedException>(() => c.BeginTransaction(IsolationLevel.Serializable));
        using DbCommand cmd = c.CreateCommand();
        cmd.Transaction = null;                                                 // allowed: there is none
        Assert.Null(cmd.Transaction);
        cmd.CommandText = "SELECT nope";
        DbException e = Assert.ThrowsAny<DbException>(() => cmd.ExecuteReader());
        Assert.IsType<SkaidbException>(e);
        Assert.Equal("unknown column: nope", e.Message);
        Assert.True(c.IsUsable);                                                // a server error is not a transport error
        Assert.Throws<InvalidOperationException>(() => new SkaidbCommand("SELECT 1").ExecuteNonQuery());
        using var closed = new SkaidbConnection(Cs(srv));
        Assert.Equal("connection is not open",
            Assert.Throws<SkaidbException>(() => closed.CreateCommand().ExecuteNonQuery()).Message);
    }

    // ---- parameters ---------------------------------------------------------------

    [Fact]
    public void Named_and_dollar_placeholders_become_positional_prepared_parameters()
    {
        const string sql = "SELECT * FROM t WHERE a = ? AND b = ? AND c = ? AND s = '@x $1' AND \"@col\" = 1 -- @c\n";
        var executed = new List<Request>();
        var s = new Scripted()
            .Prepare(sql, 3)
            .Prepare("SELECT ?, ?", 2)
            .Prepare("CALL proc(?, ?)", 2);
        s.OnExecute = (_, req) => { executed.Add(req); return new[] { F.Rows(new[] { "ok" }, new object?[] { true }) }; };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT * FROM t WHERE a = @b AND b = @A AND c = @b AND s = '@x $1' AND \"@col\" = 1 -- @c\n";
            cmd.Parameters.AddWithValue("a", "A");                              // with or without the @
            cmd.Parameters.AddWithValue("@b", 2L);
            cmd.Parameters.AddWithValue("@unused", 0L);                         // unused names are fine
            Assert.Equal(true, cmd.ExecuteScalar());
        }
        Assert.Equal(new object?[] { 2L, "A", 2L }, executed[0].Params);

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT $2, $1";
            cmd.Parameters.Add(1L);
            cmd.Parameters.Add("two");
            cmd.ExecuteScalar();
        }
        Assert.Equal(new object?[] { "two", 1L }, executed[1].Params);

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "proc";
            cmd.Parameters.Add(new SkaidbParameter("x", 5L));
            cmd.Parameters.Add(new SkaidbParameter { Value = null });
            cmd.ExecuteScalar();
        }
        Assert.Equal(new object?[] { 5L, null }, executed[2].Params);

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT @nope";
            cmd.Parameters.AddWithValue("a", 1L);
            Assert.Contains("no parameter named '@nope'", Assert.Throws<SkaidbException>(() => cmd.ExecuteScalar()).Message);
            cmd.CommandText = "SELECT ?, @a";
            Assert.Contains("cannot mix", Assert.Throws<SkaidbException>(() => cmd.ExecuteScalar()).Message);
            cmd.CommandText = "SELECT $3";
            Assert.Contains("$3 has no parameter", Assert.Throws<SkaidbException>(() => cmd.ExecuteScalar()).Message);
        }
    }

    [Fact]
    public void Parameter_collection_behaves_like_an_ADO_NET_collection()
    {
        var cmd = new SkaidbCommand();
        var ps = cmd.Parameters;
        Assert.Equal(0, ps.Add(1));                                             // a bare value is positional
        ps.Add(null);                                                           // a positional NULL
        var named = ps.AddWithValue("@name", "Ada");
        Assert.Equal(3, ps.Count);
        Assert.Same(named, ps["name"]);
        Assert.Same(named, ps["@NAME"]);
        Assert.Equal(2, ps.IndexOf("@name"));
        Assert.True(ps.Contains("name"));
        Assert.Null(ps[1].Value);
        Assert.Equal(DbType.String, named.DbType);                              // inferred
        named.DbType = DbType.AnsiString;
        Assert.Equal(DbType.AnsiString, named.DbType);
        named.ResetDbType();
        Assert.Equal(DbType.String, named.DbType);
        Assert.Throws<NotSupportedException>(() => named.Direction = ParameterDirection.Output);
        ps.AddRange(new object[] { 4L, new SkaidbParameter("z", 5L) });
        Assert.Equal(5, ps.Count);
        ps.RemoveAt("z");
        ps.Remove(named);
        ps.RemoveAt(0);
        Assert.Equal(2, ps.Count);
        Assert.Throws<IndexOutOfRangeException>(() => ps["missing"]);
        ps.Clear();
        Assert.Equal(0, ps.Count);
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.TableDirect);
    }

    // ---- data reader --------------------------------------------------------------

    [Fact]
    public void Reader_exposes_the_DbDataReader_surface()
    {
        var when = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);
        var id = Guid.Parse("123e4567-e89b-12d3-a456-426614174000");
        var s = new Scripted()
            .Answer("SELECT all", F.Rows(new[] { "i", "f", "s", "b", "ts", "u", "d", "n", "wide" },
                new object?[] { 7L, 1.5, "abc", new byte[] { 1, 2, 3 }, when, id, 12.5m, null, 1L },
                new object?[] { 8L, 2.5, "x", new byte[0], when, id, 1m, 3L, 2L }))
            .Answer("UPDATE t SET x = 1", F.Mutation(42))
            .Answer("CREATE TABLE t (PRIMARY KEY (id))", F.Ddl());
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT all";
            using var r = cmd.ExecuteReader();
            Assert.True(r.HasRows);
            Assert.Equal(-1, r.RecordsAffected);
            Assert.Equal(typeof(long), r.GetFieldType(0));
            Assert.Equal("int", r.GetDataTypeName(0));
            Assert.Equal("float", r.GetDataTypeName(1));
            Assert.Equal("timestamp", r.GetDataTypeName(4));
            Assert.Equal(typeof(long), r.GetFieldType(7));                      // first non-NULL value decides

            var schema = r.GetSchemaTable();
            Assert.Equal(9, schema.Rows.Count);
            Assert.Equal("s", schema.Rows[2][SchemaTableColumn.ColumnName]);
            Assert.Equal(2, schema.Rows[2][SchemaTableColumn.ColumnOrdinal]);
            Assert.Equal(typeof(string), schema.Rows[2][SchemaTableColumn.DataType]);

            Assert.True(r.Read());
            Assert.Equal(7, r.GetFieldValue<int>(0));
            Assert.Equal(7L, r.GetFieldValue<long>(0));
            Assert.Equal(7m, r.GetFieldValue<decimal>(0));
            Assert.Equal(1.5, r.GetFieldValue<double>(1));
            Assert.Equal(1.5f, r.GetFloat(1));
            Assert.Equal("abc", r.GetFieldValue<string>(2));
            Assert.Equal('a', r.GetChar(2));
            Assert.Equal(new byte[] { 1, 2, 3 }, r.GetFieldValue<byte[]>(3));
            Assert.Equal(3, r.GetBytes(3, 0, null, 0, 0));
            var buf = new byte[2];
            Assert.Equal(2, r.GetBytes(3, 1, buf, 0, 5));
            Assert.Equal(new byte[] { 2, 3 }, buf);
            var chars = new char[2];
            Assert.Equal(2, r.GetChars(2, 1, chars, 0, 2));
            Assert.Equal("bc", new string(chars));
            Assert.Equal(when, r.GetFieldValue<DateTimeOffset>(4));
            Assert.Equal(when.UtcDateTime, r.GetFieldValue<DateTime>(4));
            Assert.Equal(DateTimeKind.Utc, r.GetDateTime(4).Kind);
            Assert.Equal(id, r.GetFieldValue<Guid>(5));
            Assert.Equal(12.5m, r.GetFieldValue<decimal>(6));
            Assert.Null(r.GetFieldValue<long?>(7));
            Assert.Null(r.GetFieldValue<string>(7));
            Assert.Throws<SkaidbException>(() => r.GetFieldValue<long>(7));
            Assert.Equal(1L, r.GetFieldValue<long?>(8));
            var values = new object[9];
            Assert.Equal(9, r.GetValues(values));
            Assert.Equal(DBNull.Value, values[7]);
            Assert.Equal(1, r.GetInt16(8));
            Assert.Equal((byte)1, r.GetByte(8));

            Assert.True(r.Read());
            Assert.False(r.Read());
            Assert.False(r.NextResult());
            r.Close();
            Assert.True(r.IsClosed);
            Assert.Throws<InvalidOperationException>(() => r.Read());
        }

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT all";
            using DbDataReader r = cmd.ExecuteReader();
            int n = 0;
            foreach (IDataRecord rec in r) { Assert.Equal(9, rec.FieldCount); n++; }
            Assert.Equal(2, n);
        }

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "UPDATE t SET x = 1";
            using var r = cmd.ExecuteReader();
            Assert.Equal(0, r.FieldCount);
            Assert.Equal(42, r.RecordsAffected);
            Assert.False(r.HasRows);
            Assert.False(r.Read());
            cmd.CommandText = "CREATE TABLE t (PRIMARY KEY (id))";
            using var d = cmd.ExecuteReader();
            Assert.Equal(-1, d.RecordsAffected);
        }

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = "SELECT all";
            using (var r = cmd.ExecuteReader(CommandBehavior.CloseConnection)) Assert.True(r.Read());
            Assert.Equal(ConnectionState.Closed, c.State);                      // closing the reader closed it
        }
    }

    // ---- batches ------------------------------------------------------------------

    [Fact]
    public void DbBatch_merges_runs_of_one_statement_into_OP_EXECUTE_BATCH()
    {
        const string insert = "INSERT INTO t (id, name) VALUES (?, ?)";
        var batches = new List<Request>();
        var s = new Scripted()
            .Prepare(insert, 2)
            .Answer("SELECT count(*) AS n FROM t", F.Rows(new[] { "n" }, new object?[] { 3L }))
            .Answer("DELETE FROM t WHERE false", F.Mutation(0));
        s.OnBatch = (_, req) => { batches.Add(req); return new[] { F.Mutation((ulong)req.Rows.Count) }; };
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.True(c.CanCreateBatch);

        using var batch = c.CreateBatch();
        for (long i = 1; i <= 3; i++)
        {
            var bc = batch.BatchCommands.Add("INSERT INTO t (id, name) VALUES (@id, @name)");
            bc.Parameters.AddWithValue("id", i);
            bc.Parameters.AddWithValue("name", i == 3 ? null : "n" + i);
        }
        batch.BatchCommands.Add("SELECT count(*) AS n FROM t");
        batch.BatchCommands.Add("DELETE FROM t WHERE false");

        Assert.Equal(3, batch.ExecuteNonQuery());
        Assert.Single(batches);                                                // ONE round-trip for the three inserts
        Assert.Equal(3, batches[0].Rows.Count);
        Assert.Equal(new object?[] { 3L, null }, batches[0].Rows[2]);
        Assert.Equal(-1, batch.BatchCommands[0].RecordsAffected);              // merged: only the total is known
        Assert.Equal(-1, batch.BatchCommands[3].RecordsAffected);
        Assert.Equal(0, batch.BatchCommands[4].RecordsAffected);

        using (var r = batch.ExecuteReader())
        {
            Assert.Equal(3, r.RecordsAffected);
            Assert.True(r.Read());
            Assert.Equal(3L, r.GetInt64(0));
            Assert.False(r.NextResult());
        }
        Assert.Equal(3L, batch.ExecuteScalar());
        Assert.Equal(3, batches.Count);

        // Through the abstract API, as a provider-agnostic caller would.
        DbBatch generic = SkaidbFactory.Instance.CreateBatch();
        generic.Connection = c;
        var cmd = generic.CreateBatchCommand();
        cmd.CommandText = "SELECT count(*) AS n FROM t";
        generic.BatchCommands.Add(cmd);
        Assert.Equal(3L, generic.ExecuteScalar());
    }

    // ---- streaming: StreamColumns / StreamAffected --------------------------------

    [Fact]
    public void Stream_reports_a_mutation_count_and_does_not_keep_stale_columns()
    {
        var s = new Scripted()
            .Answer("SELECT id FROM t", F.Header("id"), F.Chunk(new object?[] { 1L }), F.End())
            .Answer("DELETE FROM t", F.Mutation(3))
            .Answer("CREATE TABLE t (PRIMARY KEY (id))", F.Ddl());
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Single(c.Stream("SELECT id FROM t").ToList());
        Assert.Equal(new[] { "id" }, c.StreamColumns);
        Assert.Null(c.StreamAffected);
        Assert.Empty(c.Stream("DELETE FROM t").ToList());
        Assert.Empty(c.StreamColumns);                                          // not the previous stream's
        Assert.Equal(3L, c.StreamAffected);
        Assert.Empty(c.Stream("CREATE TABLE t (PRIMARY KEY (id))").ToList());
        Assert.Null(c.StreamAffected);
    }

    // ---- Dapper -------------------------------------------------------------------

    public sealed class Person
    {
        public long Id { get; set; }
        public string? Name { get; set; }
    }

    [Fact]
    public void Dapper_queries_and_executes_with_named_parameters()
    {
        const string select = "SELECT id, name FROM people WHERE id >= ?";
        const string insert = "INSERT INTO people (id, name) VALUES (?, ?)";
        var inserted = new List<object?[]>();
        var s = new Scripted().Prepare(select, 1).Prepare(insert, 2)
            .Answer("SELECT count(*) FROM people", F.Rows(new[] { "count" }, new object?[] { 2L }));
        s.OnExecute = (sql, req) =>
        {
            if (sql == insert) { inserted.Add(req.Params.ToArray()); return new[] { F.Mutation(1) }; }
            long min = (long)req.Params[0]!;
            var rows = new[] { new object?[] { 1L, "Ada" }, new object?[] { 2L, null } }.Where(r => (long)r[0]! >= min).ToArray();
            return new[] { F.Rows(new[] { "id", "name" }, rows) };
        };
        using var srv = new FakeServer(s.Handle);
        // Dapper opens a closed connection itself and closes it afterwards.
        using var c = new SkaidbConnection(Cs(srv));

        var people = c.Query<Person>("SELECT id, name FROM people WHERE id >= @min", new { min = 1L }).ToList();
        Assert.Equal(2, people.Count);
        Assert.Equal("Ada", people[0].Name);
        Assert.Null(people[1].Name);
        Assert.Equal(ConnectionState.Closed, c.State);

        Assert.Equal(2, c.Execute("INSERT INTO people (id, name) VALUES (@Id, @Name)",
            new[] { new Person { Id = 3, Name = "Grace" }, new Person { Id = 4, Name = null } }));
        Assert.Equal(new object?[] { 3L, "Grace" }, inserted[0]);
        Assert.Equal(new object?[] { 4L, null }, inserted[1]);

        c.Open();
        Assert.Equal(2L, c.ExecuteScalar<long>("SELECT count(*) FROM people"));
        var one = c.QuerySingle<Person>("SELECT id, name FROM people WHERE id >= @min", new { min = 2L });
        Assert.Equal(2L, one.Id);
        Assert.Equal(ConnectionState.Open, c.State);                           // Dapper leaves an open connection open
    }
}
