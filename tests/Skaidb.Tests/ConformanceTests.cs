// The shared skaidb wire-protocol conformance suite.
//
// conformance/vectors.json is generated from the server's reference encoders
// (https://skaidb.org/conformance/vectors.json; the contract is
// conformance/README.md). This harness runs it against the driver's PUBLIC
// API through a scripted fake server that sends the reference bytes (never
// bytes this driver encoded) and checks the exact bytes the driver sends.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace Skaidb.Tests;

public class ConformanceTests
{
    private static readonly JsonObject V = Load();
    private readonly ITestOutputHelper _out;

    public ConformanceTests(ITestOutputHelper output) { _out = output; }

    private static JsonObject Load()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string p = Path.Combine(dir.FullName, "conformance", "vectors.json");
            if (File.Exists(p)) return JsonNode.Parse(File.ReadAllText(p))!.AsObject();
        }
        throw new FileNotFoundException("conformance/vectors.json not found above " + AppContext.BaseDirectory);
    }

    private static byte[] Hex(string h) => Convert.FromHexString(h);

    private static string ToHex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    // ---- tagged JSON <-> .NET -------------------------------------------------

    /// <summary>A value the driver surfaced, in the vectors' tagged form.</summary>
    private static JsonNode Tagged(object? v)
    {
        switch (v)
        {
            case null:
            case DBNull:
                return new JsonObject { ["null"] = true };
            case bool b:
                return new JsonObject { ["bool"] = b };
            case long l:
                return new JsonObject { ["int"] = l.ToString(CultureInfo.InvariantCulture) };
            case double d:
                return new JsonObject
                {
                    ["float"] = d,
                    ["float_bits"] = BitConverter.DoubleToInt64Bits(d).ToString("x16", CultureInfo.InvariantCulture),
                };
            case decimal m:
            {
                int[] bits = decimal.GetBits(m);
                int scale = (bits[3] >> 16) & 0x7F;
                var mag = ((BigInteger)(uint)bits[2] << 64) | ((BigInteger)(uint)bits[1] << 32) | (uint)bits[0];
                if (m < 0) mag = -mag;
                return Dec(mag, scale);
            }
            case string s:
                return new JsonObject { ["string"] = s };
            case byte[] raw:
                return new JsonObject { ["bytes"] = ToHex(raw) };
            case Guid g:
                return new JsonObject { ["uuid"] = g.ToString("D") };
            case DateTimeOffset dto:
                return new JsonObject { ["timestamp_ms"] = dto.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture) };
            case object?[] arr:
                return new JsonObject { ["array"] = new JsonArray(arr.Select(Tagged).ToArray()) };
            case Dictionary<string, object?> doc:
                return new JsonObject
                {
                    ["document"] = new JsonArray(doc.Select(kv =>
                        (JsonNode)new JsonObject { ["key"] = kv.Key, ["value"] = Tagged(kv.Value) }).ToArray()),
                };
        }
        throw new InvalidOperationException($"unmapped driver value {v.GetType().Name}");
    }

    private static JsonObject Dec(BigInteger mantissa, int scale) => new()
    {
        ["decimal"] = new JsonObject
        {
            ["mantissa"] = mantissa.ToString(CultureInfo.InvariantCulture),
            ["scale"] = scale,
        },
    };

    /// <summary>
    /// A decimal too wide for System.Decimal surfaces as its documented form,
    /// a plain decimal string; convert the expected tagged value to that form.
    /// </summary>
    private static JsonNode Documented(JsonNode expected)
    {
        if (expected is JsonObject o && o["decimal"] is JsonObject d)
        {
            var m = BigInteger.Parse(d["mantissa"]!.GetValue<string>(), CultureInfo.InvariantCulture);
            int scale = d["scale"]!.GetValue<int>();
            if (!FitsDecimal(m, scale))
            {
                string digits = BigInteger.Abs(m).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
                string text = scale == 0 ? digits : digits[..^scale] + "." + digits[^scale..];
                return new JsonObject { ["string"] = (m.Sign < 0 ? "-" : "") + text };
            }
        }
        // Anything else: the same tree with every nested value converted.
        if (expected is JsonObject obj)
        {
            var copy = new JsonObject();
            foreach (var kv in obj) copy[kv.Key] = kv.Value is null ? null : Documented(kv.Value);
            return copy;
        }
        if (expected is JsonArray arr)
            return new JsonArray(arr.Select(x => x is null ? null : Documented(x)).ToArray());
        return expected.DeepClone();
    }

    private static bool FitsDecimal(BigInteger m, int scale) =>
        scale <= 28 && BigInteger.Abs(m) <= (BigInteger.One << 96) - 1;

    /// <summary>A tagged value as the .NET value the driver binds; null when the driver cannot bind it.</summary>
    private static bool TryNative(JsonNode t, out object? value)
    {
        var o = t.AsObject();
        value = null;
        if (o.ContainsKey("null")) return true;
        if (o["bool"] is { } b) { value = b.GetValue<bool>(); return true; }
        if (o["int"] is { } i) { value = long.Parse(i.GetValue<string>(), CultureInfo.InvariantCulture); return true; }
        if (o["float_bits"] is { } fb)
        {
            value = BitConverter.Int64BitsToDouble(long.Parse(fb.GetValue<string>(), NumberStyles.HexNumber, CultureInfo.InvariantCulture));
            return true;
        }
        if (o["decimal"] is JsonObject d)
        {
            var m = BigInteger.Parse(d["mantissa"]!.GetValue<string>(), CultureInfo.InvariantCulture);
            int scale = d["scale"]!.GetValue<int>();
            if (!FitsDecimal(m, scale)) return false;
            var mag = BigInteger.Abs(m);
            value = new decimal((int)(uint)(mag & 0xFFFFFFFF), (int)(uint)((mag >> 32) & 0xFFFFFFFF),
                                (int)(uint)((mag >> 64) & 0xFFFFFFFF), m.Sign < 0, (byte)scale);
            return true;
        }
        if (o["string"] is { } s) { value = s.GetValue<string>(); return true; }
        if (o["bytes"] is { } by) { value = Hex(by.GetValue<string>()); return true; }
        if (o["uuid"] is { } u) { value = Guid.Parse(u.GetValue<string>()); return true; }
        if (o["timestamp_ms"] is { } ts)
        {
            value = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(ts.GetValue<string>(), CultureInfo.InvariantCulture));
            return true;
        }
        if (o["array"] is JsonArray arr)
        {
            var items = new object?[arr.Count];
            for (int k = 0; k < arr.Count; k++)
                if (!TryNative(arr[k]!, out items[k])) return false;
            value = items;
            return true;
        }
        if (o["document"] is JsonArray entries)
        {
            var doc = new Dictionary<string, object?>();
            foreach (var e in entries)
            {
                if (!TryNative(e!["value"]!, out var x)) return false;
                doc[e["key"]!.GetValue<string>()] = x;
            }
            value = doc;
            return true;
        }
        throw new InvalidOperationException($"unknown tagged value {t.ToJsonString()}");
    }

    private static JsonObject RowsJson(IReadOnlyList<string> columns, IEnumerable<object?[]> rows) => new()
    {
        ["columns"] = new JsonArray(columns.Select(c => (JsonNode)JsonValue.Create(c)!).ToArray()),
        ["rows"] = new JsonArray(rows.Select(r => (JsonNode)new JsonArray(r.Select(Tagged).ToArray())).ToArray()),
    };

    /// <summary>
    /// Structural equality of two JSON trees; a float is compared by its
    /// float_bits only (JSON number spelling differs between encoders).
    /// </summary>
    private static bool Same(JsonNode? a, JsonNode? b)
    {
        if (a is null || b is null) return a is null && b is null;
        if (a is JsonObject oa && b is JsonObject ob)
        {
            if (oa.ContainsKey("float_bits") && ob.ContainsKey("float_bits"))
                return Same(oa["float_bits"], ob["float_bits"]);
            if (oa.Count != ob.Count) return false;
            foreach (var kv in oa)
                if (!ob.TryGetPropertyValue(kv.Key, out var other) || !Same(kv.Value, other)) return false;
            return true;
        }
        if (a is JsonArray aa && b is JsonArray ab)
        {
            if (aa.Count != ab.Count) return false;
            for (int i = 0; i < aa.Count; i++) if (!Same(aa[i], ab[i])) return false;
            return true;
        }
        if (a is JsonValue va && b is JsonValue vb)
        {
            if (va.TryGetValue<string>(out var sa) && vb.TryGetValue<string>(out var sb)) return sa == sb;
            if (va.TryGetValue<bool>(out var ba) && vb.TryGetValue<bool>(out var bb)) return ba == bb;
            return decimal.Parse(va.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture)
                == decimal.Parse(vb.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture);
        }
        return false;
    }

    private static bool Matches(JsonObject expect, JsonObject got)
    {
        if (expect["error"] is { } err)
            return got["error"] is { } g && g.GetValue<string>().Contains(err.GetValue<string>());
        if (expect["sequence"] is JsonArray es)
        {
            if (got["sequence"] is not JsonArray gs || gs.Count != es.Count) return false;
            for (int i = 0; i < es.Count; i++) if (!Matches(es[i]!.AsObject(), gs[i]!.AsObject())) return false;
            return true;
        }
        if (expect["rows_then_error"] is JsonObject e)
            return got["rows_then_error"] is JsonObject g
                && Same(Documented(e["rows"]!), g["rows"])
                && g["error"]!.GetValue<string>().Contains(e["error"]!.GetValue<string>());
        return Same(Documented(expect), got);
    }

    // ---- the scripted fake server -----------------------------------------------

    /// <summary>
    /// One connection: the handshake per <c>outcome</c>, then the scripted
    /// exchanges. The client proof is verified with the BCL primitives, not
    /// the driver's SCRAM code. Any mismatch lands in <see cref="Error"/>.
    /// </summary>
    private sealed class Script : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly Thread _thread;
        private readonly string _outcome;
        private readonly Queue<(byte[] Request, byte[][] Responses)> _pending;
        public string? Error;
        public int Port { get; }

        public Script(string outcome, IEnumerable<(byte[], byte[][])> exchanges)
        {
            _outcome = outcome;
            _pending = new Queue<(byte[], byte[][])>(exchanges);
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _thread = new Thread(Run) { IsBackground = true };
            _thread.Start();
        }

        private void Run()
        {
            try
            {
                using var s = _listener.AcceptSocket();
                Serve(s);
            }
            catch (Exception e)
            {
                Error = $"{e.GetType().Name}: {e.Message}";
            }
        }

        private static byte[]? ReadFrame(Socket s)
        {
            byte[]? head = ReadExact(s, 4);
            if (head is null) return null;
            int n = (head[0] << 24) | (head[1] << 16) | (head[2] << 8) | head[3];
            return ReadExact(s, n);
        }

        private static byte[]? ReadExact(Socket s, int n)
        {
            var buf = new byte[n];
            int got = 0;
            while (got < n)
            {
                int r = s.Receive(buf, got, n - got, SocketFlags.None);
                if (r <= 0) return null;
                got += r;
            }
            return buf;
        }

        private static void Send(Socket s, byte[] payload) => s.Send(F.Frame(payload));

        private static (byte[] Value, int Next) Str(byte[] b, int pos)
        {
            int n = BitConverter.ToInt32(b, pos);
            return (b[(pos + 4)..(pos + 4 + n)], pos + 4 + n);
        }

        private void Serve(Socket s)
        {
            var auth = V["auth"]!.AsObject();
            var challenge = auth["challenge"]!.AsObject();
            byte[] start = ReadFrame(s) ?? throw new Exception("no AuthStart");
            if (start[0] != 10) throw new Exception("expected AuthStart");
            var (user, pos) = Str(start, 1);
            var (clientNonce, _) = Str(start, pos);
            byte[] salt = Hex(challenge["salt"]!.GetValue<string>());
            int iterations = challenge["iterations"]!.GetValue<int>();
            byte[] serverNonce = clientNonce.Concat(Encoding.UTF8.GetBytes(challenge["server_nonce_suffix"]!.GetValue<string>())).ToArray();
            var w = new BinWriter();
            w.U8(11);
            w.U32((uint)salt.Length); w.Raw(salt);
            w.U32((uint)iterations);
            w.U32((uint)serverNonce.Length); w.Raw(serverNonce);
            Send(s, w.ToArray());

            byte[] finish = ReadFrame(s) ?? throw new Exception("no AuthFinish");
            if (finish[0] != 12) throw new Exception("expected AuthFinish");
            byte[] am = Encoding.UTF8.GetBytes(string.Join("\0",
                Encoding.UTF8.GetString(user), Encoding.UTF8.GetString(clientNonce), Encoding.UTF8.GetString(serverNonce),
                ToHex(salt), iterations.ToString(CultureInfo.InvariantCulture)));
            byte[] salted = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(auth["password"]!.GetValue<string>()),
                salt, iterations, HashAlgorithmName.SHA256, 32);
            byte[] clientKey = HMACSHA256.HashData(salted, Encoding.ASCII.GetBytes("Client Key"));
            byte[] clientSig = HMACSHA256.HashData(SHA256.HashData(clientKey), am);
            byte[] proof = clientKey.Zip(clientSig, (x, y) => (byte)(x ^ y)).ToArray();
            if (!finish.AsSpan(1, 32).SequenceEqual(proof)) throw new Exception("client proof did not verify");
            byte[] serverSig = HMACSHA256.HashData(HMACSHA256.HashData(salted, Encoding.ASCII.GetBytes("Server Key")), am);

            if (_outcome == "bad_server_signature")
            {
                Send(s, new byte[] { 13, 1 }.Concat(Enumerable.Repeat((byte)0xAA, 32)).ToArray());
                return;
            }
            if (_outcome == "denied")
            {
                var denied = auth["outcomes"]!.AsArray().First(o => o!["name"]!.GetValue<string>() == "denied")!;
                Send(s, Hex(denied["payload"]!.GetValue<string>()));
                return;
            }
            Send(s, new byte[] { 13, 1 }.Concat(serverSig).ToArray());

            byte[] ddl = Hex(V["ignorable_requests"]!["ddl_payload"]!.GetValue<string>());
            while (true)
            {
                byte[]? req = ReadFrame(s);
                if (req is null)
                {
                    if (_pending.Count > 0) Error = $"never received request {ToHex(_pending.Peek().Request)}";
                    return;
                }
                if (req.Length > 0 && (req[0] == 4 || req[0] == 8)) { Send(s, ddl); continue; }
                if (_pending.Count == 0) { Error = $"unexpected extra request {ToHex(req)}"; return; }
                var (want, responses) = _pending.Dequeue();
                if (!req.AsSpan().SequenceEqual(want))
                {
                    Error = $"request mismatch:\n  got  {ToHex(req)}\n  want {ToHex(want)}";
                    return;
                }
                foreach (var r in responses) Send(s, r);
            }
        }

        public void Join()
        {
            if (!_thread.Join(TimeSpan.FromSeconds(10))) Error ??= "fake server did not finish";
            _listener.Stop();
        }

        public void Dispose() => _listener.Stop();
    }

    private static SkaidbConnection Connect(int port)
    {
        var auth = V["auth"]!.AsObject();
        var b = new SkaidbConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = port,
            Username = auth["username"]!.GetValue<string>(),
            Password = auth["password"]!.GetValue<string>(),
            Timeout = TimeSpan.FromSeconds(5),
        };
        var c = new SkaidbConnection(b.ConnectionString);
        c.Open();
        return c;
    }

    // ---- running a case's call through the public API --------------------------

    private static JsonObject Outcome(Func<JsonObject> run)
    {
        try { return run(); }
        catch (SkaidbException e) { return new JsonObject { ["error"] = e.Message }; }
    }

    private static SkaidbConsistency Level(JsonObject call) =>
        SkaidbConnection.ParseConsistency(call["consistency"]!.GetValue<string>());

    /// <summary>A command's result through the ADO.NET reader.</summary>
    private static JsonObject ReaderResult(SkaidbCommand cmd)
    {
        using var r = cmd.ExecuteReader();
        if (r.FieldCount == 0)
            return r.RecordsAffected < 0
                ? new JsonObject { ["ddl"] = true }
                : new JsonObject { ["affected"] = r.RecordsAffected.ToString(CultureInfo.InvariantCulture) };
        var sets = new List<JsonObject>();
        do
        {
            var cols = Enumerable.Range(0, r.FieldCount).Select(r.GetName).ToList();
            var rows = new List<object?[]>();
            while (r.Read())
            {
                var row = new object[r.FieldCount];
                r.GetValues(row);
                rows.Add(row);
            }
            sets.Add(RowsJson(cols, rows));
        } while (r.NextResult());
        return sets.Count > 1
            ? new JsonObject { ["result_sets"] = new JsonArray(sets.ToArray()) }
            : new JsonObject { ["rows"] = sets[0] };
    }

    private JsonObject RunCall(SkaidbConnection conn, JsonObject call)
    {
        string method = call["method"]!.GetValue<string>();
        switch (method)
        {
            case "sequence":
                return new JsonObject
                {
                    ["sequence"] = new JsonArray(call["calls"]!.AsArray().Select(c => (JsonNode)RunCall(conn, c!.AsObject())).ToArray()),
                };
            case "query":
                return Outcome(() =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = call["sql"]!.GetValue<string>();
                    cmd.Consistency = Level(call);
                    return ReaderResult(cmd);
                });
            case "execute_prepared":
                return Outcome(() =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = call["sql"]!.GetValue<string>();
                    cmd.Consistency = Level(call);
                    foreach (var p in call["params"]!.AsArray())
                    {
                        Assert.True(TryNative(p!, out var v));
                        cmd.Parameters.Add(v);
                    }
                    return ReaderResult(cmd);
                });
            case "execute_batch":
                return Outcome(() =>
                {
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = call["sql"]!.GetValue<string>();
                    cmd.Consistency = Level(call);
                    var rows = call["rows"]!.AsArray().Select(row => (IReadOnlyList<object?>)row!.AsArray().Select(p =>
                    {
                        Assert.True(TryNative(p!, out var v));
                        return v;
                    }).ToList()).ToList();
                    return new JsonObject { ["affected"] = cmd.ExecuteBatch(rows).ToString(CultureInfo.InvariantCulture) };
                });
            case "query_stream":
                return Outcome(() =>
                {
                    var rows = new List<object?[]>();
                    try
                    {
                        foreach (var row in conn.Stream(call["sql"]!.GetValue<string>())) rows.Add(row);
                    }
                    catch (SkaidbException e) when (conn.StreamColumns.Count > 0)
                    {
                        return new JsonObject
                        {
                            ["rows_then_error"] = new JsonObject { ["rows"] = RowsJson(conn.StreamColumns, rows), ["error"] = e.Message },
                        };
                    }
                    if (conn.StreamColumns.Count == 0)
                        return conn.StreamAffected is long n
                            ? new JsonObject { ["affected"] = n.ToString(CultureInfo.InvariantCulture) }
                            : new JsonObject { ["ddl"] = true };
                    return new JsonObject { ["rows"] = RowsJson(conn.StreamColumns, rows) };
                });
            default:
                // Never reached today: every call method has a public API here.
                _out.WriteLine($"SKIP call.method {method}: no driver API");
                return new JsonObject { ["skipped"] = method };
        }
    }

    // ---- the tests ---------------------------------------------------------------

    [Fact]
    public void Every_value_decodes_and_encodes()
    {
        int n = 0;
        foreach (var entry in V["values"]!.AsArray())
        {
            string name = entry!["name"]!.GetValue<string>();
            byte[] encoded = Hex(entry["encoded"]!.GetValue<string>());
            object? decoded = SkaidbConnection.DecodeValue(new BinReader(encoded));
            JsonNode want = Documented(entry["value"]!);
            Assert.True(Same(want, Tagged(decoded)), $"{name}: decoded {Tagged(decoded).ToJsonString()}, want {want.ToJsonString()}");
            if (TryNative(entry["value"]!, out var native))
                Assert.Equal(entry["encoded"]!.GetValue<string>(), ToHex(SkaidbConnection.EncodeValue(native)));
            else
                _out.WriteLine($"SKIP encode {name}: a decimal wider than System.Decimal has no bindable .NET form");
            n++;
        }
        Assert.True(n > 0);
    }

    [Fact]
    public void Scram_computations_match()
    {
        foreach (var s in V["scram"]!.AsArray())
        {
            byte[] salt = Hex(s!["salt"]!.GetValue<string>());
            int iterations = s["iterations"]!.GetValue<int>();
            string password = s["password"]!.GetValue<string>();
            byte[] am = Encoding.UTF8.GetBytes(string.Join("\0",
                s["username"]!.GetValue<string>(), s["client_nonce"]!.GetValue<string>(),
                s["server_nonce"]!.GetValue<string>(), ToHex(salt), iterations.ToString(CultureInfo.InvariantCulture)));
            Assert.Equal(s["auth_message"]!.GetValue<string>(), ToHex(am));
            Assert.Equal(s["salted_password"]!.GetValue<string>(), ToHex(SkaidbConnection.SaltedPassword(password, salt, iterations)));
            var (proof, sig) = SkaidbConnection.ScramProof(password, salt, iterations, am);
            Assert.Equal(s["client_proof"]!.GetValue<string>(), ToHex(proof));
            Assert.Equal(s["server_signature"]!.GetValue<string>(), ToHex(sig));
        }
    }

    [Fact]
    public void Auth_outcomes()
    {
        foreach (var o in V["auth"]!["outcomes"]!.AsArray())
        {
            string name = o!["name"]!.GetValue<string>();
            using var server = new Script(name, Array.Empty<(byte[], byte[][])>());
            if (o["expect"]!.GetValue<string>() == "connected")
            {
                using var c = Connect(server.Port);
                Assert.True(c.IsOpen, name);
            }
            else
            {
                var e = Assert.Throws<SkaidbException>(() => Connect(server.Port));
                if (o["reason"] is { } reason) Assert.Contains(reason.GetValue<string>(), e.Message);
            }
            server.Join();
            Assert.True(server.Error is null, $"{name}: {server.Error}");
        }
    }

    [Fact]
    public void Cases()
    {
        int n = 0;
        foreach (var node in V["cases"]!.AsArray())
        {
            var c = node!.AsObject();
            string name = c["name"]!.GetValue<string>();
            var exchanges = c["exchanges"]!.AsArray().Select(e => (
                Hex(e!["request"]!.GetValue<string>()),
                e["responses"]!.AsArray().Select(r => Hex(r!.GetValue<string>())).ToArray())).ToList();
            using var server = new Script("ok", exchanges);
            JsonObject got;
            using (var conn = Connect(server.Port))
            {
                got = RunCall(conn, c["call"]!.AsObject());
            }
            server.Join();
            Assert.True(server.Error is null, $"{name}: {server.Error}");
            var expect = c["expect"]!.AsObject();
            Assert.True(Matches(expect, got), $"{name}:\n  expected {expect.ToJsonString()}\n  got      {got.ToJsonString()}");
            n++;
        }
        Assert.True(n > 0);
        _out.WriteLine($"{n} cases run; no call.method skipped");
    }
}
