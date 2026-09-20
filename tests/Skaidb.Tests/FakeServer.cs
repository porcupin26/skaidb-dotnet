// An in-process skaidb server for the driver's unit tests: speaks the frame
// layer and the SCRAM-SHA-256 handshake for real, and answers requests with
// whatever the test scripts. No skaidb binary is involved.

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Skaidb.Tests;

/// <summary>One parsed post-handshake request (client → server payload, PROTOCOL.md §3).</summary>
public sealed class Request
{
    public byte Op;
    public byte Consistency;
    public string Sql = "";
    public uint Id;
    public List<object?> Params = new();
    public List<List<object?>> Rows = new();
    public string Name = "";
    public string Version = "";
    public byte[] Raw = Array.Empty<byte>();
}

/// <summary>Server → client payload builders (§3.2).</summary>
public static class F
{
    private static void RowsBlock(BinWriter w, string[] columns, object?[][] rows)
    {
        w.U32((uint)columns.Length);
        foreach (var c in columns) w.Str(c);
        w.U32((uint)rows.Length);
        foreach (var row in rows)
        {
            w.U32((uint)row.Length);
            foreach (var v in row)
            {
                byte[] b = SkaidbConnection.EncodeValue(v);
                w.U32((uint)b.Length);
                w.Raw(b);
            }
        }
    }

    public static byte[] Rows(string[] columns, params object?[][] rows)
    {
        var w = new BinWriter();
        w.U8(0);
        RowsBlock(w, columns, rows);
        return w.ToArray();
    }

    public static byte[] Mutation(ulong n)
    {
        var w = new BinWriter();
        w.U8(1);
        w.I64((long)n);
        return w.ToArray();
    }

    public static byte[] Ddl() => new byte[] { 2 };

    public static byte[] Error(string msg)
    {
        var w = new BinWriter();
        w.U8(3);
        w.Str(msg);
        return w.ToArray();
    }

    public static byte[] Prepared(uint id, ushort nparams)
    {
        var w = new BinWriter();
        w.U8(4);
        w.U32(id);
        w.U16(nparams);
        return w.ToArray();
    }

    public static byte[] Header(params string[] columns)
    {
        var w = new BinWriter();
        w.U8(5);
        w.U32((uint)columns.Length);
        foreach (var c in columns) w.Str(c);
        return w.ToArray();
    }

    public static byte[] Chunk(params object?[][] rows)
    {
        var w = new BinWriter();
        w.U8(6);
        w.U32((uint)rows.Length);
        foreach (var row in rows)
        {
            w.U32((uint)row.Length);
            foreach (var v in row)
            {
                byte[] b = SkaidbConnection.EncodeValue(v);
                w.U32((uint)b.Length);
                w.Raw(b);
            }
        }
        return w.ToArray();
    }

    public static byte[] End() => new byte[] { 7 };

    public static byte[] ResultSets(params (string[] Columns, object?[][] Rows)[] sets)
    {
        var w = new BinWriter();
        w.U8(8);
        w.U32((uint)sets.Length);
        foreach (var (columns, rows) in sets) RowsBlock(w, columns, rows);
        return w.ToArray();
    }

    /// <summary>A whole frame: big-endian length prefix + payload (§1).</summary>
    public static byte[] Frame(byte[] payload)
    {
        var f = new byte[4 + payload.Length];
        f[0] = (byte)(payload.Length >> 24);
        f[1] = (byte)(payload.Length >> 16);
        f[2] = (byte)(payload.Length >> 8);
        f[3] = (byte)payload.Length;
        Array.Copy(payload, 0, f, 4, payload.Length);
        return f;
    }
}

public sealed class FakeConnection
{
    internal readonly Socket Sock;
    internal string State = "start";
    internal byte[]? Salted;
    internal byte[]? AuthMessage;
    public string User = "";

    internal FakeConnection(Socket s) { Sock = s; }

    public void Send(byte[] payload)
    {
        byte[] f = F.Frame(payload);
        Sock.Send(f);
    }
}

/// <summary>
/// Handler for every post-handshake request. Return the payloads to send, or
/// null to let the server answer with its default (Ddl for Hello, an Error
/// otherwise).
/// </summary>
public delegate IEnumerable<byte[]>? Handler(Request req, FakeConnection conn);

public sealed class FakeServer : IDisposable
{
    public string Password = "secret";
    public Handler Handle;
    public int Iterations = 1000;
    public bool CorruptServerSignature;

    /// <summary>Every parsed post-handshake request, in order.</summary>
    public readonly List<Request> Requests = new();
    public readonly List<Request> Hellos = new();
    /// <summary>Raw bytes of the first frame each connection sent, header included.</summary>
    public readonly List<byte[]> FirstFrames = new();
    private int _connections;
    public int Connections => Volatile.Read(ref _connections);

    private readonly TcpListener _listener;
    private readonly List<Socket> _sockets = new();
    private readonly object _gate = new();
    private volatile bool _stopped;

    public int Port { get; }

    public FakeServer(Handler? handle = null, string? password = null)
    {
        Handle = handle ?? ((req, conn) => null);
        if (password is not null) Password = password;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        var t = new Thread(AcceptLoop) { IsBackground = true, Name = "fake-server-accept" };
        t.Start();
    }

    /// <summary>Kill every live connection, as a node restart would.</summary>
    public void CloseAll()
    {
        List<Socket> copy;
        lock (_gate) { copy = new List<Socket>(_sockets); _sockets.Clear(); }
        foreach (var s in copy)
        {
            try { s.Shutdown(SocketShutdown.Both); } catch { /* ignore */ }
            try { s.Close(); } catch { /* ignore */ }
        }
    }

    public void Dispose()
    {
        _stopped = true;
        try { _listener.Stop(); } catch { /* ignore */ }
        CloseAll();
    }

    private void AcceptLoop()
    {
        while (!_stopped)
        {
            Socket s;
            try { s = _listener.AcceptSocket(); }
            catch { return; }
            Interlocked.Increment(ref _connections);
            lock (_gate) _sockets.Add(s);
            var t = new Thread(() => Serve(s)) { IsBackground = true, Name = "fake-server-conn" };
            t.Start();
        }
    }

    private static bool ReadExact(Socket s, byte[] buf, int n)
    {
        int got = 0;
        while (got < n)
        {
            int r;
            try { r = s.Receive(buf, got, n - got, SocketFlags.None); }
            catch { return false; }
            if (r <= 0) return false;
            got += r;
        }
        return true;
    }

    private void Serve(Socket s)
    {
        var conn = new FakeConnection(s);
        bool first = true;
        try
        {
            var head = new byte[4];
            while (true)
            {
                if (!ReadExact(s, head, 4)) return;
                int len = (head[0] << 24) | (head[1] << 16) | (head[2] << 8) | head[3];
                var payload = new byte[len];
                if (!ReadExact(s, payload, len)) return;
                if (first)
                {
                    first = false;
                    var raw = new byte[4 + len];
                    Array.Copy(head, raw, 4);
                    Array.Copy(payload, 0, raw, 4, len);
                    lock (_gate) FirstFrames.Add(raw);
                }
                try { OnFrame(conn, payload); }
                catch (Exception e) { conn.Send(F.Error("fake server: " + e.Message)); }
            }
        }
        catch { /* connection gone */ }
        finally
        {
            lock (_gate) _sockets.Remove(s);
            try { s.Close(); } catch { /* ignore */ }
        }
    }

    private void OnFrame(FakeConnection conn, byte[] payload)
    {
        var r = new BinReader(payload);
        if (conn.State == "start")
        {
            if (r.U8() != 10) throw new Exception("expected AuthStart");
            string user = r.Text();
            string clientNonce = r.Text();
            byte[] salt = RandomNumberGenerator.GetBytes(16);
            string serverNonce = "s" + Guid.NewGuid().ToString("N");
            conn.User = user;
            string saltHex = Convert.ToHexString(salt).ToLowerInvariant();
            conn.AuthMessage = Encoding.UTF8.GetBytes(string.Join("\0", user, clientNonce, serverNonce, saltHex, Iterations.ToString()));
            conn.Salted = Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(Password), salt, Iterations, HashAlgorithmName.SHA256, 32);
            var w = new BinWriter();
            w.U8(11);
            w.U32((uint)salt.Length); w.Raw(salt);
            w.U32((uint)Iterations);
            w.Str(serverNonce);
            conn.Send(w.ToArray());
            conn.State = "finish";
            return;
        }
        if (conn.State == "finish")
        {
            if (r.U8() != 12) throw new Exception("expected AuthFinish");
            byte[] proof = r.Take(32);
            byte[] clientKey = HMACSHA256.HashData(conn.Salted!, Encoding.ASCII.GetBytes("Client Key"));
            byte[] storedKey = SHA256.HashData(clientKey);
            byte[] clientSig = HMACSHA256.HashData(storedKey, conn.AuthMessage!);
            var recovered = new byte[32];
            for (int i = 0; i < 32; i++) recovered[i] = (byte)(proof[i] ^ clientSig[i]);
            if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(recovered), storedKey))
            {
                var d = new BinWriter();
                d.U8(13); d.U8(0); d.Str("bad password");
                conn.Send(d.ToArray());
                try { conn.Sock.Shutdown(SocketShutdown.Both); } catch { /* ignore */ }
                return;
            }
            byte[] serverKey = HMACSHA256.HashData(conn.Salted!, Encoding.ASCII.GetBytes("Server Key"));
            byte[] serverSig = HMACSHA256.HashData(serverKey, conn.AuthMessage!);
            if (CorruptServerSignature) { serverSig = new byte[32]; Array.Fill(serverSig, (byte)7); }
            var ok = new BinWriter();
            ok.U8(13); ok.U8(1); ok.Raw(serverSig);
            conn.Send(ok.ToArray());
            conn.State = "ready";
            return;
        }
        var req = Parse(payload);
        lock (_gate)
        {
            Requests.Add(req);
            if (req.Op == 8) Hellos.Add(req);
        }
        IEnumerable<byte[]>? outs = Handle(req, conn);
        if (outs is null)
            outs = new[] { req.Op == 8 ? F.Ddl() : F.Error($"fake server: unhandled op {req.Op}") };
        foreach (var p in outs) conn.Send(p);
    }

    /// <summary>Parse a client → server payload (§3).</summary>
    public static Request Parse(byte[] payload)
    {
        var r = new BinReader(payload);
        var req = new Request { Op = r.U8(), Raw = payload };
        switch (req.Op)
        {
            case 1:
            case 5:
                req.Consistency = r.U8();
                req.Sql = r.Text();
                break;
            case 2:
                req.Sql = r.Text();
                break;
            case 3:
            {
                req.Consistency = r.U8();
                req.Id = r.U32();
                int n = r.U16();
                for (int i = 0; i < n; i++) req.Params.Add(SkaidbConnection.DecodeValue(new BinReader(r.Blob())));
                break;
            }
            case 4:
                req.Id = r.U32();
                break;
            case 7:
            {
                req.Consistency = r.U8();
                req.Id = r.U32();
                uint nrows = r.U32();
                for (uint i = 0; i < nrows; i++)
                {
                    int n = r.U16();
                    var row = new List<object?>();
                    for (int j = 0; j < n; j++) row.Add(SkaidbConnection.DecodeValue(new BinReader(r.Blob())));
                    req.Rows.Add(row);
                }
                break;
            }
            case 8:
                req.Name = r.Text();
                req.Version = r.Text();
                break;
        }
        return req;
    }
}

/// <summary>
/// A scripted handler: <c>script[sql]</c> answers OP_QUERY / OP_QUERY_STREAM
/// text, <c>prepared[sql]</c> makes OP_PREPARE succeed with that arity,
/// executes go through <c>onExecute</c>, batches through <c>onBatch</c>.
/// </summary>
public sealed class Scripted
{
    public readonly Dictionary<string, Func<Request, FakeConnection, IEnumerable<byte[]>>> Script = new();
    public readonly Dictionary<string, ushort> Prepared = new();
    public Func<string, Request, IEnumerable<byte[]>>? OnExecute;
    public Func<string, Request, IEnumerable<byte[]>>? OnBatch;
    public readonly Dictionary<uint, string> Ids = new();
    private uint _nextId = 100;

    public Scripted Answer(string sql, params byte[][] payloads)
    {
        Script[sql] = (_, _) => payloads;
        return this;
    }

    public Scripted Answer(string sql, Func<Request, byte[]> f)
    {
        Script[sql] = (req, _) => new[] { f(req) };
        return this;
    }

    public Scripted Prepare(string sql, ushort nparams)
    {
        Prepared[sql] = nparams;
        return this;
    }

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        switch (req.Op)
        {
            case 1:
            case 5:
                if (!Script.TryGetValue(req.Sql, out var a)) return new[] { F.Error("fake server: unknown statement " + req.Sql) };
                return a(req, conn);
            case 2:
                if (!Prepared.TryGetValue(req.Sql, out var n)) return new[] { F.Error("statement kind cannot be prepared") };
                uint id = _nextId++;
                Ids[id] = req.Sql;
                return new[] { F.Prepared(id, n) };
            case 3:
                return OnExecute is null ? new[] { F.Error("unexpected execute") } : OnExecute(Ids[req.Id], req);
            case 7:
                return OnBatch is null ? new[] { F.Mutation((ulong)req.Rows.Count) } : OnBatch(Ids[req.Id], req);
            default:
                return null;   // Hello -> Ddl
        }
    }
}
