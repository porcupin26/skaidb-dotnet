// Client-side parameter binding (PROTOCOL.md §5): the fallback for
// statements the server will not prepare.

using System;
using System.Collections.Generic;
using System.Numerics;
using Xunit;

namespace Skaidb.Tests;

public class BindTests
{
    private static string Bind(string sql, params object?[] p) => ParameterBinder.Bind(sql, new List<object?>(p));

    [Fact]
    public void Quotes_every_scalar_type()
    {
        Assert.Equal("SELECT NULL, NULL, TRUE, FALSE", Bind("SELECT ?, ?, ?, ?", null, DBNull.Value, true, false));
        Assert.Equal("SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9", Bind("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?",
            (byte)1, (sbyte)2, (short)3, (ushort)4, 5, 6u, 7L, 8UL, new BigInteger(9)));
        Assert.Equal("SELECT 1.5, 2.25, 3.125", Bind("SELECT ?, ?, ?", 1.5f, 2.25, 3.125m));
        Assert.Equal("SELECT 'O''Brien', 'x', ''''", Bind("SELECT ?, ?, ?", "O'Brien", 'x', '\''));
        Assert.Equal("SELECT '00fe'", Bind("SELECT ?", new byte[] { 0, 0xfe }));
        Assert.Equal("SELECT '0f8fad5b-d9cb-469f-a165-70867728950e'", Bind("SELECT ?", Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e")));
        var ts = new DateTimeOffset(2026, 9, 19, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal($"SELECT {ts.ToUnixTimeMilliseconds()}", Bind("SELECT ?", ts));
        Assert.Equal($"SELECT {ts.ToUnixTimeMilliseconds()}", Bind("SELECT ?", ts.UtcDateTime));
        Assert.Equal($"SELECT {ts.ToUnixTimeMilliseconds()}", Bind("SELECT ?", DateTime.SpecifyKind(ts.UtcDateTime, DateTimeKind.Unspecified)));
    }

    [Fact]
    public void Placeholders_inside_string_literals_are_left_alone()
    {
        Assert.Equal("SELECT 'a?b', 1", Bind("SELECT 'a?b', ?", 1));
        Assert.Equal("SELECT 'it''s ?', 2", Bind("SELECT 'it''s ?', ?", 2));
        Assert.Equal("SELECT 'a?b'", Bind("SELECT 'a?b'"));
    }

    [Fact]
    public void Count_mismatches_are_errors()
    {
        Assert.Contains("more placeholders than parameters", Assert.Throws<SkaidbException>(() => Bind("SELECT ?, ?", 1)).Message);
        Assert.Contains("more parameters than placeholders", Assert.Throws<SkaidbException>(() => Bind("SELECT ?", 1, 2)).Message);
        Assert.Contains("placeholders but no parameters", Assert.Throws<SkaidbException>(() => Bind("SELECT ?")).Message);
        Assert.Equal("SELECT 1", Bind("SELECT 1"));
    }

    [Fact]
    public void Arrays_documents_and_NaN_cannot_be_rendered_as_text()
    {
        Assert.Throws<SkaidbException>(() => Bind("SELECT ?", new object?[] { new object?[] { 1 } }));
        Assert.Throws<SkaidbException>(() => Bind("SELECT ?", new Dictionary<string, object?>()));
        Assert.Throws<SkaidbException>(() => Bind("SELECT ?", double.NaN));
        Assert.Throws<SkaidbException>(() => Bind("SELECT ?", float.PositiveInfinity));
    }
}
