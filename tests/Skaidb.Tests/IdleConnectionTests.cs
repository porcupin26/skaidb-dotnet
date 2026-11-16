// A connection that died while it sat idle, as a client on a real network
// meets it: the peer's end of the socket is gone, the client's writes to it
// still succeed, and only a read would find out. The fake server half-closes
// (it stops sending, keeps the socket open and counts what still arrives),
// or resets, and the tests assert where the next statement went.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Xunit;

namespace Skaidb.Tests;

public class IdleConnectionTests
{
    private const string Update = "UPDATE accounts SET balance = balance - 100 WHERE id = 1";
    private const string Second = "UPDATE accounts SET balance = balance + 100 WHERE id = 2";
    private const string Bound = "UPDATE accounts SET balance = balance - ? WHERE id = ?";

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

    private static object? Scalar(SkaidbConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static void WaitFor(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            Thread.Sleep(10);
        }
    }

    /// <summary>
    /// The peer's end of every connection goes away while <paramref name="idle"/>
    /// sit idle: half-closed (EOF for the client's reads, its writes still
    /// accepted) or reset. Returns once each of them could know.
    /// </summary>
    private static void Die(FakeServer srv, string how, params SkaidbConnection[] idle)
    {
        if (how == "half-closed") srv.HalfCloseAll(idle);
        else srv.ResetAll(idle);
    }

    /// <summary>Every way a statement is run, each with the answer the fake gives it.</summary>
    private static void Run(string way, SkaidbConnection c)
    {
        var rows = new List<IReadOnlyList<object?>> { new object?[] { 100L, 1L }, new object?[] { 100L, 2L } };
        switch (way)
        {
            case "a read":
                Assert.Equal(7L, Scalar(c, "SELECT balance FROM accounts WHERE id = 1"));
                break;
            case "a write":
                Assert.Equal(1, Exec(c, Update));
                break;
            case "a prepared write":
                Assert.Equal(1, Exec(c, Bound, 100L, 1L));
                break;
            case "Prepare()":
                using (var cmd = c.CreateCommand())
                {
                    cmd.CommandText = Bound;
                    cmd.Parameters.Add(100L);
                    cmd.Parameters.Add(1L);
                    cmd.Prepare();
                    Assert.Equal(1, cmd.ExecuteNonQuery());
                }
                break;
            case "ExecuteBatch":
                using (var cmd = c.CreateCommand())
                {
                    cmd.CommandText = Bound;
                    Assert.Equal(2L, cmd.ExecuteBatch(rows));
                }
                break;
            case "a DbBatch":
                using (var batch = c.CreateBatch())
                {
                    batch.BatchCommands.Add(Update);
                    batch.BatchCommands.Add(Second);
                    Assert.Equal(2, batch.ExecuteNonQuery());
                }
                break;
            case "a stream":
                Assert.Equal(new[] { 1L, 2L }, c.Stream("SELECT id FROM accounts").Select(r => (long)r[0]!));
                break;
            case "ChangeDatabase()":
                c.ChangeDatabase("app");
                break;
            default:
                Assert.Equal(1, c.Transaction(conn => Exec(conn, Update)));
                break;
        }
    }

    public static IEnumerable<object[]> EveryWay() =>
        from way in new[]
        {
            "a read", "a write", "a prepared write", "Prepare()", "ExecuteBatch", "a DbBatch", "a stream",
            "ChangeDatabase()", "Transaction()",
        }
        from how in new[] { "half-closed", "reset" }
        select new object[] { way, how };

    [Theory]
    [MemberData(nameof(EveryWay))]
    public void A_connection_that_died_while_it_sat_idle_costs_no_statement(string way, string how)
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        var dead = srv.Accepted.Single();
        Die(srv, how, c);

        // The statement finds out before it writes anything, takes a new
        // connection and runs there. No error reaches the caller: nothing
        // was sent on the dead socket, so there is nothing that may have run.
        Run(way, c);

        Assert.Equal(2, srv.Connections);
        var second = srv.Accepted[1];
        Assert.All(s.Sockets.Skip(1), socket => Assert.Same(second, socket));     // every statement since, on the new one
        Assert.All(srv.Requests.Where(q => q.Via == dead), q => Assert.True(q.Op == 8 || q.Sql == "SELECT 1"));
        if (how == "half-closed")
        {
            // Not a byte: the write would have succeeded, and the statement
            // with it would have been lost with the read that followed.
            Assert.Equal(0, dead.ReceivedSinceHalfClose);
            // The driver closed the dead socket when it found it (rule 4).
            WaitFor(() => srv.Live == 1, "the dead socket is still open on the client's side");
        }
        Assert.True(c.IsUsable);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        Assert.Equal(2, srv.Connections);
    }

    [Theory]
    [InlineData("half-closed")]
    [InlineData("reset")]
    public void A_pooled_connection_that_died_while_it_sat_idle_is_not_handed_out(string how)
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv));
        SkaidbConnection? idle = null;
        Assert.Equal(7L, pool.WithConnection(c => Scalar(idle = c, "SELECT 1")));
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
        var dead = srv.Accepted.Single();
        Die(srv, how, idle!);

        // Idle in the pool: the checkout looks, drops it and gives a working one.
        var got = pool.Acquire();
        Assert.NotSame(idle, got);
        Assert.False(idle!.IsOpen);
        Assert.True(got.IsUsable);
        Assert.Equal(1, Exec(got, Update));
        Assert.Equal(2, srv.Connections);
        Assert.Same(srv.Accepted[1], s.Sockets[^1]);
        if (how == "half-closed") Assert.Equal(0, dead.ReceivedSinceHalfClose);

        // Idle in a borrower's hands: its next statement re-dials.
        var deadToo = srv.Accepted[1];
        Die(srv, how, got);
        Assert.Equal(1, Exec(got, Second));
        Assert.Equal(3, srv.Connections);
        Assert.Same(srv.Accepted[2], s.Sockets[^1]);
        if (how == "half-closed") Assert.Equal(0, deadToo.ReceivedSinceHalfClose);
        pool.Release(got);
        Assert.Equal(1, pool.Members().Sum(m => m.Idle));
        Assert.Equal(0, pool.Members().Sum(m => m.InUse));
        Assert.Equal(new[] { "SELECT 1", Update, Second }, s.Log);
    }

    [Theory]
    [InlineData("half-closed", false)]
    [InlineData("reset", false)]
    [InlineData("half-closed", true)]
    [InlineData("reset", true)]
    public void Inside_Transaction_a_connection_that_died_while_idle_is_a_lost_transaction_not_a_re_dial(string how, bool pooled)
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var pool = new SkaidbConnectionPool(Cs(srv));
        using var single = new SkaidbConnection(Cs(srv));
        if (!pooled) single.Open();
        FakeConnection? dead = null;
        Action<SkaidbConnection> body = conn =>
        {
            Exec(conn, Update);
            dead = srv.Accepted.Single();
            Die(srv, how, conn);
            Exec(conn, Second);                                                  // rule 7: not on a new session
        };
        var lost = Assert.Throws<SkaidbException>(() =>
        {
            if (pooled) pool.Transaction(body);
            else single.Transaction(body);
        });
        Assert.Contains("connection lost inside a transaction", lost.Message);
        Assert.Equal(new[] { "BEGIN", Update }, s.Log);                          // nothing since, anywhere
        Assert.Equal(1, srv.Connections);
        if (how == "half-closed") Assert.Equal(0, dead!.ReceivedSinceHalfClose);
        // The helper has returned: the next statement gets a new connection.
        Assert.Equal(7L, pooled ? pool.WithConnection(c => Scalar(c, "SELECT 1")) : Scalar(single, "SELECT 1"));
        Assert.Equal(2, srv.Connections);
    }

    [Fact]
    public void A_frame_nobody_asked_for_on_an_idle_connection_is_not_read_as_the_next_statement_s_reply()
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        Assert.Equal(7L, Scalar(c, "SELECT 1"));
        // An idle connection is owed nothing. Bytes that turn up on one mean
        // the two ends no longer agree where a reply starts: whatever is
        // asked next would be answered with them.
        srv.Accepted.Single().Send(F.Ddl());
        SpinWait.SpinUntil(() => !c.IsUsable, TimeSpan.FromSeconds(2));
        Assert.Equal(7L, Scalar(c, "SELECT 1"));                                 // its own reply, not the stray frame (no rows)
        Assert.Equal(1, Exec(c, Update));
        Assert.Equal(2, srv.Connections);                                        // on a new connection: the other was dropped
        Assert.Equal(new[] { "SELECT 1", "SELECT 1", Update }, s.Log);
        Assert.NotSame(s.Sockets[0], s.Sockets[1]);
        Assert.Same(s.Sockets[1], s.Sockets[2]);
        WaitFor(() => srv.Live == 1, "the connection with the stray frame on it is still open");
    }

    [Fact]
    public void A_live_idle_connection_is_looked_at_without_a_round_trip_and_stays()
    {
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle);
        using var c = Connect(srv);
        for (int i = 0; i < 5; i++)
        {
            Assert.True(c.IsUsable);
            Assert.Equal(7L, Scalar(c, "SELECT 1"));
        }
        // Five statements, five requests behind the Hello: the look before
        // each one is a poll of the socket, nothing the server sees.
        Assert.Equal(5, srv.Requests.Count(q => q.Op != 8));
        Assert.Equal(1, srv.Connections);
    }

    private static X509Certificate2 SelfSigned()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var req = new CertificateRequest("CN=skaidb", key, HashAlgorithmName.SHA256);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("skaidb");
        req.CertificateExtensions.Add(san.Build());
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        // SslStream on some platforms wants a persisted key for a server cert.
        return new X509Certificate2(cert.Export(X509ContentType.Pkcs12));
    }

    [Fact]
    public void Under_TLS_the_look_is_at_the_transport_socket()
    {
        using var cert = SelfSigned();
        var s = new Standalone();
        using var srv = new FakeServer(s.Handle) { Certificate = cert };
        using var c = Connect(srv, "TlsInsecure=true");
        Assert.True(c.Tls);
        // Live: nothing the TLS session itself exchanges is taken for a dead
        // peer, and nothing is read out from under it.
        for (int i = 0; i < 3; i++)
        {
            Assert.True(c.IsUsable);
            Assert.Equal(7L, Scalar(c, "SELECT 1"));
        }
        Assert.Equal(1, srv.Connections);

        // The TCP connection half-closes under the session (no TLS alert is
        // sent): the socket shows it, the decrypted stream would not until
        // a read.
        var dead = srv.Accepted.Single();
        srv.HalfCloseAll(c);
        Assert.Equal(1, Exec(c, Update));
        Assert.Equal(2, srv.Connections);
        Assert.Same(srv.Accepted[1], s.Sockets[^1]);
        Assert.Equal(0, dead.ReceivedSinceHalfClose);
        Assert.True(c.IsUsable);
    }
}
