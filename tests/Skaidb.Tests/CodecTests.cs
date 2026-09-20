// The value codec (PROTOCOL.md §4): every tag encodes to the documented
// bytes and decodes back to the same .NET value.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Xunit;

namespace Skaidb.Tests;

public class CodecTests
{
    private static object? RoundTrip(object? v) =>
        SkaidbConnection.DecodeValue(new BinReader(SkaidbConnection.EncodeValue(v)));

    private static byte[] Enc(object? v) => SkaidbConnection.EncodeValue(v);

    [Fact]
    public void Null_and_DBNull_encode_to_tag_0_and_decode_to_null()
    {
        Assert.Equal(new byte[] { 0 }, Enc(null));
        Assert.Equal(new byte[] { 0 }, Enc(DBNull.Value));
        Assert.Null(RoundTrip(null));
    }

    [Fact]
    public void Bool()
    {
        Assert.Equal(new byte[] { 1, 1 }, Enc(true));
        Assert.Equal(new byte[] { 1, 0 }, Enc(false));
        Assert.Equal(true, RoundTrip(true));
        Assert.Equal(false, RoundTrip(false));
    }

    [Fact]
    public void Int64_is_little_endian_and_every_integer_width_binds_as_Int()
    {
        Assert.Equal(new byte[] { 2, 1, 0, 0, 0, 0, 0, 0, 0 }, Enc(1L));
        Assert.Equal(new byte[] { 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }, Enc(-1L));
        Assert.Equal(new byte[] { 2, 0x04, 0x03, 0x02, 0x01, 0, 0, 0, 0 }, Enc(0x01020304));
        foreach (object v in new object[] { (sbyte)5, (byte)5, (short)5, (ushort)5, 5, 5u, 5L, 5UL })
            Assert.Equal(5L, RoundTrip(v));
        Assert.Equal(long.MaxValue, RoundTrip(long.MaxValue));
        Assert.Equal(long.MinValue, RoundTrip(long.MinValue));
        Assert.Equal(long.MaxValue, RoundTrip((ulong)long.MaxValue));
    }

    [Fact]
    public void Float64_is_IEEE_bits_little_endian_and_refuses_NaN()
    {
        Assert.Equal(new byte[] { 3, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f }, Enc(1.5));
        Assert.Equal(1.5, RoundTrip(1.5));
        Assert.Equal(-0.0, RoundTrip(-0.0));
        Assert.Equal(2.5, RoundTrip(2.5f));
        Assert.Equal(double.MaxValue, RoundTrip(double.MaxValue));
        Assert.Throws<SkaidbException>(() => Enc(double.NaN));
        Assert.Throws<SkaidbException>(() => Enc(double.PositiveInfinity));
        Assert.Throws<SkaidbException>(() => Enc(float.NaN));
    }

    [Fact]
    public void Decimal_is_i128_mantissa_little_endian_then_u32_scale()
    {
        // 12345.6789 = 123456789 / 10^4
        byte[] b = Enc(12345.6789m);
        Assert.Equal(4, b[0]);
        Assert.Equal(1 + 16 + 4, b.Length);
        Assert.Equal(new BigInteger(123456789), new BigInteger(b.AsSpan(1, 16), isUnsigned: false, isBigEndian: false));
        Assert.Equal(4u, BitConverter.ToUInt32(b, 17));
        Assert.Equal(12345.6789m, RoundTrip(12345.6789m));
        // Negative mantissa is sign-extended over the full 16 bytes.
        byte[] n = Enc(-0.5m);
        Assert.Equal(new BigInteger(-5), new BigInteger(n.AsSpan(1, 16), isUnsigned: false, isBigEndian: false));
        Assert.Equal(1u, BitConverter.ToUInt32(n, 17));
        Assert.Equal(-0.5m, RoundTrip(-0.5m));
        Assert.Equal(0m, RoundTrip(0m));
        Assert.Equal(decimal.MaxValue, RoundTrip(decimal.MaxValue));
        Assert.Equal(decimal.MinValue, RoundTrip(decimal.MinValue));
        // Trailing zeros are part of the scale and survive.
        Assert.Equal("1.500", RoundTrip(1.500m)!.ToString());
    }

    [Fact]
    public void Decimal_too_wide_for_System_Decimal_decodes_to_an_exact_string()
    {
        // 2^100 with scale 3: exceeds the 96-bit magnitude.
        var w = new BinWriter();
        w.U8(4);
        byte[] le = (BigInteger.One << 100).ToByteArray();
        var wide = new byte[16];
        Array.Copy(le, wide, le.Length);
        w.Raw(wide);
        w.U32(3);
        object? v = SkaidbConnection.DecodeValue(new BinReader(w.ToArray()));
        Assert.Equal("1267650600228229401496703205.376", v);
        // Scale beyond 28 also falls back.
        var w2 = new BinWriter();
        w2.U8(4);
        var one = new byte[16]; one[0] = 1;
        w2.Raw(one);
        w2.U32(30);
        Assert.Equal("0.000000000000000000000000000001", SkaidbConnection.DecodeValue(new BinReader(w2.ToArray())));
    }

    [Fact]
    public void BigInteger_and_large_ulong_bind_as_Int_when_they_fit_else_Decimal()
    {
        Assert.Equal(2, Enc(new BigInteger(42))[0]);
        Assert.Equal(42L, RoundTrip(new BigInteger(42)));
        BigInteger big = BigInteger.Pow(2, 70);
        Assert.Equal(4, Enc(big)[0]);
        Assert.Equal(big.ToString(), RoundTrip(big)!.ToString());   // string fallback: > 96 bits
        Assert.Equal(4, Enc(ulong.MaxValue)[0]);
        Assert.Equal(18446744073709551615m, RoundTrip(ulong.MaxValue));
        Assert.Throws<SkaidbException>(() => Enc(BigInteger.Pow(2, 130)));
    }

    [Fact]
    public void String_is_u32_length_then_utf8()
    {
        Assert.Equal(new byte[] { 5, 2, 0, 0, 0, 0xc3, 0xa9 }, Enc("é"));
        Assert.Equal("O'Brien ünïcödé 🎉", RoundTrip("O'Brien ünïcödé 🎉"));
        Assert.Equal("", RoundTrip(""));
        Assert.Equal("x", RoundTrip('x'));
    }

    [Fact]
    public void Bytes()
    {
        Assert.Equal(new byte[] { 6, 3, 0, 0, 0, 0, 0xfe, 0xff }, Enc(new byte[] { 0, 0xfe, 0xff }));
        Assert.Equal(new byte[] { 0, 0xfe, 0xff }, RoundTrip(new byte[] { 0, 0xfe, 0xff }));
        Assert.Equal(Array.Empty<byte>(), RoundTrip(Array.Empty<byte>()));
    }

    [Fact]
    public void Uuid_is_16_bytes_in_RFC_4122_order_not_Guid_ToByteArray_order()
    {
        var g = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");
        byte[] b = Enc(g);
        Assert.Equal(7, b[0]);
        Assert.Equal("0f8fad5bd9cb469fa16570867728950e", Convert.ToHexString(b, 1, 16).ToLowerInvariant());
        Assert.Equal(g, RoundTrip(g));
        Assert.Equal(Guid.Empty, RoundTrip(Guid.Empty));
    }

    [Fact]
    public void Timestamp_is_unix_milliseconds_and_decodes_as_UTC_DateTimeOffset()
    {
        var ts = new DateTimeOffset(2026, 9, 19, 12, 34, 56, 789, TimeSpan.Zero);
        byte[] b = Enc(ts);
        Assert.Equal(8, b[0]);
        Assert.Equal(ts.ToUnixTimeMilliseconds(), BitConverter.ToInt64(b, 1));
        var back = (DateTimeOffset)RoundTrip(ts)!;
        Assert.Equal(ts, back);
        Assert.Equal(TimeSpan.Zero, back.Offset);
        // An offset input is normalised: same instant.
        Assert.Equal(ts, RoundTrip(ts.ToOffset(TimeSpan.FromHours(2))));
        // DateTime: UTC kind, and Unspecified is taken as UTC.
        Assert.Equal(ts, RoundTrip(ts.UtcDateTime));
        Assert.Equal(ts, RoundTrip(DateTime.SpecifyKind(ts.UtcDateTime, DateTimeKind.Unspecified)));
        // Before 1970.
        var old = new DateTimeOffset(1969, 12, 31, 23, 59, 59, 0, TimeSpan.Zero);
        Assert.Equal(old, RoundTrip(old));
    }

    [Fact]
    public void Array_nests_and_any_IEnumerable_binds()
    {
        var arr = new object?[] { 1L, "two", 3.5, true, null, new object?[] { 9L } };
        byte[] b = Enc(arr);
        Assert.Equal(9, b[0]);
        Assert.Equal(6u, BitConverter.ToUInt32(b, 1));
        var back = (object?[])RoundTrip(arr)!;
        Assert.Equal(6, back.Length);
        Assert.Equal(1L, back[0]);
        Assert.Equal("two", back[1]);
        Assert.Equal(3.5, back[2]);
        Assert.Equal(true, back[3]);
        Assert.Null(back[4]);
        Assert.Equal(9L, ((object?[])back[5]!)[0]);
        Assert.Equal(new byte[] { 9, 0, 0, 0, 0 }, Enc(Array.Empty<object>()));
        // A List<int> and a string[] are arrays too.
        var fromList = (object?[])RoundTrip(new List<int> { 1, 2 })!;
        Assert.Equal(new object?[] { 1L, 2L }, fromList);
        Assert.Equal(new object?[] { "a" }, (object?[])RoundTrip(new[] { "a" })!);
    }

    [Fact]
    public void Document_keeps_wire_order_and_requires_string_keys()
    {
        // In-process encode -> decode keeps the order the keys were written in.
        // A stored document comes back from the server with its keys sorted
        // (the server canonicalises on write); tests/Live asserts that.
        var doc = new Dictionary<string, object?> { ["z"] = 1L, ["a"] = "x", ["n"] = null, ["sub"] = new Dictionary<string, object?> { ["k"] = 2.5 } };
        byte[] b = Enc(doc);
        Assert.Equal(10, b[0]);
        Assert.Equal(4u, BitConverter.ToUInt32(b, 1));
        var back = (Dictionary<string, object?>)RoundTrip(doc)!;
        Assert.Equal(new[] { "z", "a", "n", "sub" }, back.Keys.ToArray());
        Assert.Equal(1L, back["z"]);
        Assert.Equal("x", back["a"]);
        Assert.Null(back["n"]);
        Assert.Equal(2.5, ((Dictionary<string, object?>)back["sub"]!)["k"]);
        Assert.Equal(new byte[] { 10, 0, 0, 0, 0 }, Enc(new Dictionary<string, object?>()));
        var bad = new Dictionary<int, object?> { [1] = 1 };
        Assert.Throws<SkaidbException>(() => Enc(bad));
    }

    [Fact]
    public void Unknown_types_and_truncated_input_are_refused()
    {
        Assert.Throws<SkaidbException>(() => Enc(new object()));
        Assert.Throws<SkaidbException>(() => SkaidbConnection.DecodeValue(new BinReader(new byte[] { 99 })));
        Assert.Throws<SkaidbException>(() => SkaidbConnection.DecodeValue(new BinReader(new byte[] { 2, 1, 2 })));
        Assert.Throws<SkaidbException>(() => SkaidbConnection.DecodeValue(new BinReader(new byte[] { 5, 9, 0, 0, 0, 0x61 })));
        Assert.Throws<SkaidbException>(() => SkaidbConnection.DecodeValue(new BinReader(Array.Empty<byte>())));
    }

    [Fact]
    public void BinWriter_and_BinReader_are_little_endian_inside_payloads()
    {
        var w = new BinWriter();
        w.U8(7); w.U16(0x0102); w.U32(0x01020304); w.I64(-2); w.Str("ab"); w.Raw(new byte[] { 9 });
        byte[] b = w.ToArray();
        Assert.Equal(new byte[] { 7, 2, 1, 4, 3, 2, 1, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0, 0x61, 0x62, 9 }, b);
        var r = new BinReader(b);
        Assert.Equal(7, r.U8());
        Assert.Equal(0x0102, r.U16());
        Assert.Equal(0x01020304u, r.U32());
        Assert.Equal(-2L, r.I64());
        Assert.Equal("ab", r.Text());
        Assert.Equal(new byte[] { 9 }, r.Take(1));
        Assert.Throws<SkaidbException>(() => r.U8());
        Assert.Equal(ulong.MaxValue, new BinReader(new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff }).U64());
    }

    [Fact]
    public void Frame_header_is_big_endian()
    {
        byte[] f = F.Frame(new byte[300]);
        Assert.Equal(new byte[] { 0, 0, 1, 44 }, f.AsSpan(0, 4).ToArray());
        Assert.Equal(304, f.Length);
    }
}
