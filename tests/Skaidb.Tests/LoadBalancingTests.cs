// Pool load balancing (PROTOCOL.md 6.1): policies, idle order, health
// ejection and re-admission, member discovery (SHOW ENDPOINTS) and
// read-your-writes affinity — against fake members that each serve many
// connections.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using Xunit;

namespace Skaidb.Tests;

/// <summary>
/// A fake skaidb member: the fake server's SCRAM handshake on every
/// connection, then <c>SELECT …</c> answers one row naming the member (after
/// <see cref="DelayMs"/>), <c>SHOW ENDPOINTS</c> answers <see cref="Listing"/>,
/// <c>SHOW DATABASES</c> (the pool's probe) answers no rows, anything else is
/// a one-row mutation. <see cref="Refuse"/> closes each new connection before
/// the handshake; <see cref="OldServer"/> answers SHOW ENDPOINTS with a parse
/// error.
/// </summary>
public sealed class Member : IDisposable
{
    public readonly string Name;
    public readonly FakeServer Server;
    public volatile int DelayMs;
    public volatile bool OldServer;
    public Func<IEnumerable<object?[]>>? Listing;

    public Member(string name)
    {
        Name = name;
        Server = new FakeServer(Handle);
    }

    public int Port => Server.Port;
    public string Endpoint => $"127.0.0.1:{Port}";

    public bool Refuse
    {
        get => Server.Refuse;
        set => Server.Refuse = value;
    }

    private IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        if (req.Op != 1) return null;   // Hello -> Ddl
        if (req.Sql == "SHOW ENDPOINTS")
        {
            var listing = Listing;
            if (OldServer || listing is null) return new[] { F.Error("parse error: expected TABLES, found ENDPOINTS") };
            return new[] { F.Rows(new[] { "endpoint", "state", "local" }, listing().ToArray()) };
        }
        if (req.Sql == "SHOW DATABASES") return new[] { F.Rows(new[] { "name" }) };
        if (req.Sql.StartsWith("SELECT", StringComparison.Ordinal))
        {
            int delay = DelayMs;
            if (delay > 0) Thread.Sleep(delay);
            return new[] { F.Rows(new[] { "member" }, new object?[] { Name }) };
        }
        return new[] { F.Mutation(1) };
    }

    public void Dispose() => Server.Dispose();
}

public sealed class LoadBalancingTests : IDisposable
{
    private readonly Member _a = new("a");
    private readonly Member _b = new("b");
    private readonly Member _c = new("c");
    private Member[] All => new[] { _a, _b, _c };

    public void Dispose()
    {
        foreach (var m in All) m.Dispose();
    }

    private static SkaidbConnectionPool Pool(IEnumerable<Member> seeds, SkaidbPoolOptions? options = null, string? hosts = null)
    {
        string h = hosts ?? string.Join(",", seeds.Select(m => m.Endpoint));
        return new SkaidbConnectionPool($"Host={h};User=ada;Password=secret;Timeout=5", options ?? new SkaidbPoolOptions());
    }

    private static string Who(SkaidbConnectionPool pool, object? affinity = null) =>
        pool.WithConnection(affinity, c =>
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT member()";
            return (string)cmd.ExecuteScalar()!;
        });

    private static SkaidbPoolMember Row(SkaidbConnectionPool pool, Member m) =>
        pool.Members().Single(r => r.Endpoint == m.Endpoint);

    private static void WaitFor(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, what);
            Thread.Sleep(50);
        }
    }

    // ---- policies -------------------------------------------------------------

    [Fact]
    public void Round_robin_rotates_over_every_member()
    {
        using var p = Pool(All, new SkaidbPoolOptions { LoadBalancing = SkaidbLoadBalancing.RoundRobin });
        var served = Enumerable.Range(0, 9).Select(_ => Who(p)).ToList();
        Assert.Equal("aaabbbccc", string.Concat(served.OrderBy(s => s, StringComparer.Ordinal)));
        // Strict rotation: three in a row are three different members.
        for (int i = 0; i < 9; i += 3) Assert.Equal(3, served.Skip(i).Take(3).Distinct().Count());
    }

    [Fact]
    public void Least_latency_prefers_the_faster_member()
    {
        _b.DelayMs = 20;
        using var p = Pool(new[] { _a, _b }, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.LeastLatency,
            EjectLatencyFactor = 0,
        });
        var served = Enumerable.Range(0, 60).Select(_ => Who(p)).ToList();
        Assert.True(served.Count(s => s == "a") > 48, string.Join(",", served));
        Assert.Contains("b", served);   // b was measured at least once
    }

    [Fact]
    public void Least_latency_re_measures_a_member_it_has_not_used_lately()
    {
        _b.DelayMs = 20;
        using var p = Pool(new[] { _a, _b }, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.LeastLatency,
            EjectLatencyFactor = 0,
        });
        for (int i = 0; i < 20; i++) Who(p);
        // b is slow, then recovers: an idle pool notices once b's average
        // goes stale instead of never asking b again.
        _b.DelayMs = 0;
        p.MemberAt(_b.Endpoint)!.LastSample -= 10;
        var served = Enumerable.Range(0, 3).Select(_ => Who(p)).ToList();
        Assert.Contains("b", served);
    }

    [Fact]
    public void Random_policy_reuses_one_connection_as_before()
    {
        using var p = Pool(All);
        // One connection, reused: random never targets a member.
        Assert.Single(Enumerable.Range(0, 5).Select(_ => Who(p)).Distinct());
    }

    [Fact]
    public void Invalid_options_are_refused()
    {
        string cs = "Host=127.0.0.1";
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool(cs, new SkaidbPoolOptions { LoadBalancing = (SkaidbLoadBalancing)7 }));
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool(cs, new SkaidbPoolOptions { IdleOrder = (SkaidbIdleOrder)7 }));
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool(cs, new SkaidbPoolOptions { EjectErrors = 0 }));
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool(cs, new SkaidbPoolOptions { MaxSize = 0 }));
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool(cs, new SkaidbPoolOptions { EjectBackoff = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<SkaidbException>(() => new SkaidbConnectionPool("Host=127.0.0.1;Nope=1", new SkaidbPoolOptions()));
        Assert.Throws<ArgumentNullException>(() => new SkaidbConnectionPool(cs, (SkaidbPoolOptions)null!));
    }

    // ---- idle order -------------------------------------------------------------

    private (SkaidbConnectionPool, SkaidbConnection[]) Three(SkaidbPoolOptions options)
    {
        var p = Pool(new[] { _a }, options);
        var conns = new[] { p.Acquire(), p.Acquire(), p.Acquire() };
        foreach (var c in conns) p.Release(c);
        return (p, conns);
    }

    [Fact]
    public void Lifo_hands_out_the_most_recently_returned()
    {
        var (p, conns) = Three(new SkaidbPoolOptions());
        using (p) Assert.Same(conns[2], p.Acquire());
    }

    [Fact]
    public void Fifo_hands_out_the_longest_idle()
    {
        var (p, conns) = Three(new SkaidbPoolOptions { IdleOrder = SkaidbIdleOrder.Fifo });
        using (p)
        {
            var got = new[] { p.Acquire(), p.Acquire(), p.Acquire() };
            Assert.Equal(conns, got);
        }
    }

    // ---- ejection -------------------------------------------------------------

    [Fact]
    public void A_failing_member_is_ejected_then_readmitted_by_the_probe()
    {
        _b.Refuse = true;
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            EjectErrors = 2,
            EjectBackoff = TimeSpan.FromMilliseconds(300),
        });
        var served = Enumerable.Range(0, 12).Select(_ => Who(p)).ToList();
        Assert.DoesNotContain("b", served);
        Assert.Equal(SkaidbMemberState.Ejected, Row(p, _b).State);
        // Healthy again: the background probe re-admits it after the backoff.
        _b.Refuse = false;
        WaitFor(() => Row(p, _b).State == SkaidbMemberState.Ready, "never re-admitted");
        served = Enumerable.Range(0, 6).Select(_ => Who(p)).ToList();
        Assert.Contains("b", served);
    }

    [Fact]
    public void A_probe_that_fails_doubles_the_backoff()
    {
        _b.Refuse = true;
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            EjectErrors = 1,
            EjectBackoff = TimeSpan.FromMilliseconds(200),
        });
        for (int i = 0; i < 6; i++) Who(p);
        var m = p.MemberAt(_b.Endpoint)!;
        WaitFor(() => Volatile.Read(ref m.Ejections) >= 2, "the probe never ran");
        Assert.Equal(SkaidbMemberState.Ejected, Row(p, _b).State);
        Assert.True(p.BackoffFor(m) >= 0.4);
    }

    [Fact]
    public void A_consistently_slow_member_is_ejected()
    {
        _b.DelayMs = 50;
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            EjectLatencyFloor = TimeSpan.FromMilliseconds(20),
            EjectBackoff = TimeSpan.FromSeconds(30),
        });
        for (int i = 0; i < 45; i++) Who(p);
        Assert.Equal(SkaidbMemberState.Ejected, Row(p, _b).State);
        Assert.DoesNotContain("b", Enumerable.Range(0, 6).Select(_ => Who(p)).ToList());
    }

    [Fact]
    public void An_ejected_member_is_not_ejected_again_by_late_round_trips()
    {
        _b.DelayMs = 50;
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            EjectLatencyFloor = TimeSpan.FromMilliseconds(20),
            EjectBackoff = TimeSpan.FromSeconds(30),
        });
        // A connection to b checked out before the ejection, and kept.
        SkaidbConnection? late = null;
        for (int i = 0; i < 3; i++)
        {
            var c = p.Acquire();
            if (late is null && c.ConnectedEndpoint == _b.Endpoint) late = c;
            else p.Release(c);
        }
        Assert.NotNull(late);
        for (int i = 0; i < 45; i++) Who(p);
        Assert.Equal(SkaidbMemberState.Ejected, Row(p, _b).State);
        var m = p.MemberAt(_b.Endpoint)!;
        double? until = m.EjectedUntil;
        // Slow round trips still finishing on it.
        using (var cmd = late!.CreateCommand())
        {
            cmd.CommandText = "SELECT member()";
            for (int i = 0; i < 5; i++) Assert.Equal("b", cmd.ExecuteScalar());
        }
        Assert.Equal(1, m.Ejections);
        Assert.Equal(until, m.EjectedUntil);
        p.Release(late);
        Assert.False(late.IsOpen);   // its member is ejected: not kept
    }

    [Fact]
    public void One_slow_statement_does_not_eject_a_member()
    {
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            EjectLatencyFloor = TimeSpan.FromMilliseconds(50),
        });
        for (int i = 0; i < 36; i++) Who(p);
        _b.DelayMs = 500;
        while (Who(p) != "b") { }
        _b.DelayMs = 0;
        for (int i = 0; i < 6; i++) Who(p);
        Assert.Equal(SkaidbMemberState.Ready, Row(p, _b).State);
    }

    [Fact]
    public void The_last_usable_member_is_never_ejected()
    {
        _b.Refuse = _c.Refuse = true;
        using var p = Pool(All, new SkaidbPoolOptions { LoadBalancing = SkaidbLoadBalancing.RoundRobin, EjectErrors = 1 });
        for (int i = 0; i < 6; i++) Assert.Equal("a", Who(p));
        _a.Refuse = true;
        // Idle connections to a still work (how many depends on the dial
        // order above); once they are used up, every dial fails.
        var held = new List<SkaidbConnection>();
        try
        {
            Assert.Throws<SkaidbException>(() =>
            {
                for (int i = 0; i < 12; i++) held.Add(p.Acquire());
            });
            Assert.Equal(SkaidbMemberState.Ready, Row(p, _a).State);
        }
        finally
        {
            foreach (var c in held) p.Release(c);
        }
    }

    [Fact]
    public void A_refused_login_is_not_a_member_fault()
    {
        Assert.False(SkaidbConnectionPool.MemberFault(new SkaidbException("authentication denied: bad password")));
        Assert.True(SkaidbConnectionPool.MemberFault(new SkaidbException("authentication denied: too many connections: 500")));
        Assert.False(SkaidbConnectionPool.MemberFault(new SkaidbException("server signature mismatch (mutual auth failed)")));
        Assert.True(SkaidbConnectionPool.MemberFault(new SkaidbException("connection closed by server")));
        Assert.True(SkaidbConnectionPool.MemberFault(new SocketException((int)SocketError.ConnectionRefused)));
        Assert.True(SkaidbConnectionPool.MemberFault(new IOException("reset")));
    }

    [Fact]
    public void A_refused_login_fails_the_checkout_without_ejecting_anyone()
    {
        using var p = new SkaidbConnectionPool(
            $"Host={string.Join(",", All.Select(m => m.Endpoint))};User=ada;Password=wrong;Timeout=5",
            new SkaidbPoolOptions { LoadBalancing = SkaidbLoadBalancing.RoundRobin, EjectErrors = 1 });
        for (int i = 0; i < 3; i++)
            Assert.Contains("authentication denied", Assert.Throws<SkaidbException>(() => p.Acquire()).Message);
        Assert.All(p.Members(), r => Assert.Equal(SkaidbMemberState.Ready, r.State));
        // Each checkout stopped at the member that refused the login.
        Assert.Equal(3, All.Sum(m => m.Server.Connections));
    }

    [Fact]
    public void A_seed_that_drops_the_handshake_is_skipped()
    {
        _a.Refuse = true;
        using var c = new SkaidbConnection($"Seeds={_a.Endpoint},{_b.Endpoint};User=ada;Password=secret;Timeout=5");
        for (int i = 0; i < 4; i++)
        {
            c.Open();
            using (var cmd = c.CreateCommand())
            {
                cmd.CommandText = "SELECT member()";
                Assert.Equal("b", cmd.ExecuteScalar());
            }
            c.Close();
        }
        _b.Refuse = true;
        var e = Assert.Throws<SkaidbException>(() => c.Open());
        Assert.StartsWith("no reachable endpoint in ", e.Message);
    }

    // ---- introspection --------------------------------------------------------

    [Fact]
    public void Members_reports_checkouts_idle_connections_and_latency()
    {
        using var p = Pool(new[] { _a }, new SkaidbPoolOptions { LoadBalancing = SkaidbLoadBalancing.RoundRobin });
        var first = p.Acquire();
        var second = p.Acquire();
        var row = Row(p, _a);
        Assert.Equal(2, row.InUse);
        Assert.Equal(0, row.Idle);
        Assert.True(row.Seed);
        Assert.Null(row.Canonical);
        Assert.Null(row.Latency);
        p.Release(first);
        second.Dispose();   // disposed instead of released: no longer counted
        row = Row(p, _a);
        Assert.Equal(0, row.InUse);
        Assert.Equal(1, row.Idle);
        Who(p);
        Assert.NotNull(Row(p, _a).Latency);
    }

    // ---- discovery -------------------------------------------------------------

    private readonly Dictionary<string, string> _states = new();

    private void ListAll()
    {
        lock (_states)
        {
            _states[_a.Endpoint] = "ready";
            _states[_b.Endpoint] = "ready";
            _states[_c.Endpoint] = "resyncing";
        }
        foreach (var m in All)
        {
            var me = m;
            m.Listing = () =>
            {
                lock (_states)
                {
                    return _states.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                        .Select(kv => new object?[] { kv.Key, kv.Value, kv.Key == me.Endpoint })
                        .ToList();
                }
            };
        }
    }

    private void SetState(Member m, string? state)
    {
        lock (_states)
        {
            if (state is null) _states.Remove(m.Endpoint);
            else _states[m.Endpoint] = state;
        }
    }

    private static void Relist(SkaidbConnectionPool p)
    {
        var conn = p.Acquire();
        try { p.ListThrough(conn); }
        finally { p.Release(conn); }
    }

    private static readonly SkaidbPoolOptions Discovering = new()
    {
        LoadBalancing = SkaidbLoadBalancing.RoundRobin,
        Discover = true,
    };

    [Fact]
    public void Members_are_discovered_and_resyncing_ones_get_no_connections()
    {
        ListAll();
        using var p = Pool(new[] { _a }, Discovering);
        var served = Enumerable.Range(0, 8).Select(_ => Who(p)).ToHashSet();
        Assert.Equal(new HashSet<string> { "a", "b" }, served);
        Assert.Equal(SkaidbMemberState.Resyncing, Row(p, _c).State);
        // c catches up: the next refresh lets it in, measured afresh.
        var m = p.MemberAt(_c.Endpoint)!;
        m.Ewma = 9.9;
        m.Samples = 50;
        SetState(_c, "ready");
        Relist(p);
        Assert.Null(m.Ewma);
        Assert.Equal(0, m.Samples);
        Assert.Contains("c", Enumerable.Range(0, 6).Select(_ => Who(p)).ToList());
    }

    [Fact]
    public void A_listing_with_a_member_not_ready_refreshes_sooner()
    {
        ListAll();
        using var p = Pool(new[] { _a }, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            Discover = true,
            RefreshInterval = TimeSpan.FromSeconds(60),
        });
        Who(p);
        Assert.True(p.NextRefreshIn <= 5.0, $"{p.NextRefreshIn}");   // c resyncing
        SetState(_c, "ready");
        Relist(p);
        Assert.True(p.NextRefreshIn > 50.0, $"{p.NextRefreshIn}");
    }

    [Fact]
    public void A_seed_given_by_another_name_is_not_counted_twice()
    {
        ListAll();
        using var p = Pool(new[] { _a }, Discovering, hosts: $"localhost:{_a.Port}");
        Who(p);
        var rows = p.Members();
        Assert.Equal(3, rows.Count);
        var seed = rows.Single(r => r.Seed);
        Assert.Equal($"localhost:{_a.Port}", seed.Endpoint);
        Assert.Equal(_a.Endpoint, seed.Canonical);
    }

    [Fact]
    public void A_member_that_leaves_the_listing_is_dropped_and_a_seed_is_kept_unlisted()
    {
        ListAll();
        using var p = Pool(new[] { _a }, Discovering);
        Who(p);
        SetState(_b, null);
        Relist(p);
        Assert.DoesNotContain(_b.Endpoint, p.Members().Select(r => r.Endpoint));
        // b comes back and the seed leaves the ring: the seed is kept, marked
        // unlisted, and gets no new connections.
        SetState(_b, "ready");
        SetState(_a, null);
        Relist(p);
        Assert.Equal(SkaidbMemberState.Unlisted, Row(p, _a).State);
        Assert.Equal(SkaidbMemberState.Ready, Row(p, _b).State);
        Assert.Equal(new HashSet<string> { "b" }, Enumerable.Range(0, 4).Select(_ => Who(p)).ToHashSet());
    }

    [Fact]
    public void An_old_server_turns_discovery_off_and_keeps_the_seeds()
    {
        ListAll();
        _a.OldServer = true;
        using var p = Pool(new[] { _a }, Discovering);
        Assert.Equal(new HashSet<string> { "a" }, Enumerable.Range(0, 4).Select(_ => Who(p)).ToHashSet());
        Assert.True(p.DiscoveryOff);
        Assert.Single(p.Members());
    }

    [Fact]
    public void A_standalone_server_turns_discovery_off_and_keeps_the_seeds()
    {
        _a.Listing = () => Array.Empty<object?[]>();
        using var p = Pool(new[] { _a }, Discovering);
        Assert.Equal(new HashSet<string> { "a" }, Enumerable.Range(0, 4).Select(_ => Who(p)).ToHashSet());
        Assert.True(p.DiscoveryOff);
        Assert.Single(_a.Server.Requests, r => r.Sql == "SHOW ENDPOINTS");   // asked once, then never again
        var row = Assert.Single(p.Members());
        Assert.Equal(SkaidbMemberState.Ready, row.State);
        Assert.Null(row.Canonical);
    }

    // ---- read-your-writes ------------------------------------------------------

    [Fact]
    public void Reads_follow_the_member_that_took_the_write()
    {
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            ReadYourWrites = TimeSpan.FromMilliseconds(400),
        });
        string wrote = p.WithConnection("alice", c =>
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "INSERT INTO t VALUES (1)";
            cmd.ExecuteNonQuery();
            return c.ConnectedEndpoint!;
        });
        string writer = All.Single(m => m.Endpoint == wrote).Name;
        Assert.Equal(new[] { writer }, Enumerable.Range(0, 6).Select(_ => Who(p, "alice")).Distinct());
        // Another key, or no key, still rotates.
        Assert.Equal(3, Enumerable.Range(0, 3).Select(_ => Who(p, "bob")).Distinct().Count());
        Thread.Sleep(500);
        Assert.Equal(3, Enumerable.Range(0, 3).Select(_ => Who(p, "alice")).Distinct().Count());
    }

    [Fact]
    public void Reads_do_not_set_affinity()
    {
        using var p = Pool(All, new SkaidbPoolOptions
        {
            LoadBalancing = SkaidbLoadBalancing.RoundRobin,
            ReadYourWrites = TimeSpan.FromSeconds(5),
        });
        Who(p, "alice");
        Assert.Equal(3, Enumerable.Range(0, 3).Select(_ => Who(p, "alice")).Distinct().Count());
    }

    [Theory]
    [InlineData("SELECT 1", false)]
    [InlineData("  select 1", false)]
    [InlineData("(SELECT 1)", false)]
    [InlineData("-- c\nSHOW TABLES", false)]
    [InlineData("/* x */ EXPLAIN SELECT 1", false)]
    [InlineData("WITH a AS (SELECT 1) SELECT * FROM a", false)]
    [InlineData("DESCRIBE t", false)]
    [InlineData("VALUES (1)", false)]
    [InlineData("use app", false)]
    [InlineData("INSERT INTO t VALUES (1)", true)]
    [InlineData("update t set a = 1", true)]
    [InlineData("DELETE FROM t", true)]
    [InlineData("CREATE TABLE t (id INT PRIMARY KEY)", true)]
    [InlineData("COMMIT", true)]
    [InlineData("CALL p()", true)]
    [InlineData("selectx", true)]
    public void Statement_classification(string sql, bool write)
    {
        Assert.Equal(write, SkaidbConnectionPool.IsWrite(sql));
    }
}
