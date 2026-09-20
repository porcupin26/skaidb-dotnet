using System;
using Xunit;

namespace Skaidb.Tests;

public class ConnectionStringTests
{
    [Fact]
    public void Defaults()
    {
        var c = new SkaidbConnection("");
        Assert.Equal("localhost", c.Host);
        Assert.Equal(7000, c.Port);
        Assert.Equal("anonymous", c.User);
        Assert.Equal(SkaidbConsistency.Quorum, c.Consistency);
        Assert.Equal(TimeSpan.FromSeconds(10), c.Timeout);
        Assert.Equal("", c.Database);
        Assert.False(c.Tls);
        Assert.Equal("skaidb", c.TlsServerName);
        Assert.Equal(new[] { "localhost:7000" }, c.Seeds);
        Assert.False(c.IsOpen);
        Assert.False(c.IsUsable);
    }

    [Fact]
    public void Keys_and_aliases_are_case_insensitive()
    {
        var c = new SkaidbConnection("Server=db1; PORT=7001; User Id=ada; Pwd=s;consistency=all;Connect Timeout=2.5;DB=app");
        Assert.Equal("db1", c.Host);
        Assert.Equal(7001, c.Port);
        Assert.Equal("ada", c.User);
        Assert.Equal(SkaidbConsistency.All, c.Consistency);
        Assert.Equal(TimeSpan.FromSeconds(2.5), c.Timeout);
        Assert.Equal("app", c.Database);
        Assert.Equal(SkaidbConsistency.One, new SkaidbConnection("Consistency=one").Consistency);
        Assert.Equal(SkaidbConsistency.Quorum, new SkaidbConnection("Consistency=1").Consistency);
        Assert.Equal("h", new SkaidbConnection("Data Source=h").Host);
        Assert.Equal("h", new SkaidbConnection("DataSource=h").Host);
    }

    [Fact]
    public void Tls_is_implied_by_TlsCa_or_TlsInsecure()
    {
        Assert.True(new SkaidbConnection("Tls=true").Tls);
        Assert.True(new SkaidbConnection("Tls=1").Tls);
        var ca = new SkaidbConnection("TlsCa=/etc/skaidb/ca.crt");
        Assert.True(ca.Tls);
        Assert.Equal("/etc/skaidb/ca.crt", ca.TlsCa);
        var ins = new SkaidbConnection("TlsInsecure=true;TlsServerName=db1.example");
        Assert.True(ins.Tls);
        Assert.True(ins.TlsInsecure);
        Assert.Equal("db1.example", ins.TlsServerName);
        Assert.True(new SkaidbConnection(tlsCa: "/x.pem").Tls);
    }

    [Fact]
    public void Seeds_split_and_trim()
    {
        var c = new SkaidbConnection("Seeds= db1:7000 , db2:7001,db3 ;User=u");
        Assert.Equal(new[] { "db1:7000", "db2:7001", "db3" }, c.Seeds);
    }

    [Fact]
    public void Malformed_input_is_a_SkaidbException()
    {
        Assert.Throws<SkaidbException>(() => new SkaidbConnection("Nope=1"));
        Assert.Throws<SkaidbException>(() => new SkaidbConnection("garbage"));
        Assert.Throws<SkaidbException>(() => new SkaidbConnection("Consistency=maybe"));
        Assert.Throws<ArgumentNullException>(() => new SkaidbConnection((string)null!));
    }

    [Fact]
    public void Named_arguments_constructor()
    {
        var c = new SkaidbConnection(host: "h", port: 1, user: "u", password: "p", consistency: SkaidbConsistency.One, database: "d");
        Assert.Equal("h", c.Host);
        Assert.Equal(1, c.Port);
        Assert.Equal("u", c.User);
        Assert.Equal(SkaidbConsistency.One, c.Consistency);
        Assert.Equal("d", c.Database);
        Assert.Equal(new[] { "h:1" }, c.Seeds);
    }
}
