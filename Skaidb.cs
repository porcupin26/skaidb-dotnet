// skaidb — official C# / .NET driver.
//
// Pure BCL (no NuGet packages). ADO.NET-shaped API with concrete Skaidb* types:
//
//     using var conn = new SkaidbConnection("Host=localhost;Port=7000;User=skaidb;Password=secret");
//     conn.Open();
//     using var cmd = conn.CreateCommand();
//     cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
//     cmd.Parameters.Add(1);
//     cmd.Parameters.Add("Ada");
//     cmd.ExecuteNonQuery();
//
//     cmd = conn.CreateCommand();
//     cmd.CommandText = "SELECT id, name FROM users WHERE id = ?";
//     cmd.Parameters.Add(1);
//     using var reader = cmd.ExecuteReader();
//     while (reader.Read())
//         Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
//
// Placeholders use the `?` (qmark) style, bound positionally from Parameters.
// The wire protocol is documented in ../PROTOCOL.md; this driver is verified
// against the live-tested Python reference in ../python/skaidb/__init__.py and
// is byte-for-byte compatible with it.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Numerics;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;

namespace Skaidb;

/// <summary>Consistency level for a query (how many replicas must agree).</summary>
public enum SkaidbConsistency : byte
{
    One = 0,
    Quorum = 1,
    All = 2,
}

/// <summary>Thrown on any driver or server-reported error.</summary>
/// <summary>
/// Statement kind the server declines to prepare (DDL, session statements).
/// Not an error — the caller falls back to client-side text binding.
/// </summary>
public sealed class UnpreparableException : SkaidbException
{
    public UnpreparableException(string message) : base(message) { }
}

public class SkaidbException : Exception
{
    public SkaidbException(string message) : base(message) { }
    public SkaidbException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A connection to one skaidb node. Construct with a connection string or named
/// arguments, then call <see cref="Open"/> (idempotent) to run the SCRAM handshake.
/// </summary>
public sealed class SkaidbConnection : IDisposable
{
    public string Host { get; }
    public int Port { get; }
    public string User { get; }
    private readonly string _password;

    /// <summary>Default consistency for commands created from this connection.</summary>
    public SkaidbConsistency Consistency { get; set; }

    /// <summary>Connect/read timeout. Default 10s. Set before <see cref="Open"/>.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    private TcpClient? _tcp;
    private Stream? _stream;
    private bool _open;
    private bool _disposed;
    private readonly object _lock = new();
    private readonly Dictionary<string, (uint Id, int Params)> _prepared = new();

    /// <summary>Transport died; the next statement re-dials (see EnsureLive).</summary>
    private bool _broken;

    /// <summary>
    /// A <see cref="Stream"/> owns the socket until RowsEnd/Error. Written
    /// under <see cref="_lock"/>, together with the check that guards a
    /// request. Volatile because <see cref="IsUsable"/> reads it from whatever
    /// thread a pool happens to return the connection on.
    /// </summary>
    private volatile bool _streaming;

    /// <summary>
    /// Frames an abandoned stream reads and discards before giving up:
    /// enough to finish off a stream the caller merely peeked at, and far
    /// short of dragging a whole abandoned export through the socket just to
    /// keep one connection warm. Past the cap the connection is broken
    /// instead, which is the other half of "drain the remaining frames ...
    /// or close it" (PROTOCOL.md 3) and costs one re-dial.
    ///
    /// This bounds FRAMES, not bytes. At the server's current 256 KB
    /// chunking it is ~16 MB, but that is the server's constant and not one
    /// this driver can see; <see cref="ReadFrame"/> accepts up to 64 MB, so
    /// the arithmetic worst case is far larger. Quoting the megabytes as a
    /// guarantee would be asserting a property of a number we do not
    /// control.
    /// </summary>
    private const int StreamDrainFrames = 64;

    private static int _nonceCounter;

    /// <summary>Parse a connection string such as
    /// "Host=localhost;Port=7000;User=skaidb;Password=secret;Consistency=Quorum".
    /// Keys are case-insensitive.</summary>
    public SkaidbConnection(string connectionString)
    {
        if (connectionString is null) throw new ArgumentNullException(nameof(connectionString));

        string host = "localhost";
        int port = 7000;
        string user = "anonymous";
        string password = "";
        SkaidbConsistency consistency = SkaidbConsistency.Quorum;
        TimeSpan? timeout = null;
        string database = "";
        bool tls = false, tlsInsecure = false;
        string tlsCa = "", tlsServerName = "skaidb", seeds = "";

        foreach (var rawPart in connectionString.Split(';'))
        {
            var part = rawPart.Trim();
            if (part.Length == 0) continue;
            int eq = part.IndexOf('=');
            if (eq < 0) throw new SkaidbException($"malformed connection string segment: '{part}'");
            string key = part.Substring(0, eq).Trim();
            string val = part.Substring(eq + 1).Trim();
            switch (key.ToLowerInvariant())
            {
                case "host":
                case "server":
                case "datasource":
                case "data source":
                    host = val;
                    break;
                case "port":
                    port = int.Parse(val, CultureInfo.InvariantCulture);
                    break;
                case "user":
                case "username":
                case "userid":
                case "user id":
                case "uid":
                    user = val;
                    break;
                case "password":
                case "pwd":
                    password = val;
                    break;
                case "consistency":
                    consistency = ParseConsistency(val);
                    break;
                case "database":
                case "db":
                    database = val;
                    break;
                case "tls":
                    tls = val.Equals("true", StringComparison.OrdinalIgnoreCase) || val == "1";
                    break;
                case "tlsca":
                case "tls ca":
                    tlsCa = val;
                    break;
                case "tlsinsecure":
                case "tls insecure":
                    tlsInsecure = val.Equals("true", StringComparison.OrdinalIgnoreCase) || val == "1";
                    break;
                case "tlsservername":
                case "tls server name":
                    tlsServerName = val;
                    break;
                case "seeds":
                    seeds = val;
                    break;
                case "timeout":
                case "connecttimeout":
                case "connect timeout":
                    timeout = TimeSpan.FromSeconds(double.Parse(val, CultureInfo.InvariantCulture));
                    break;
                default:
                    throw new SkaidbException($"unknown connection string key: '{key}'");
            }
        }

        Host = host;
        Port = port;
        Database = database;
        // A server with client_tls = required refuses plaintext outright, so
        // without TLS such a cluster is unreachable. Any knob turns it on.
        Tls = tls || tlsCa.Length > 0 || tlsInsecure;
        TlsCa = tlsCa;
        TlsInsecure = tlsInsecure;
        TlsServerName = tlsServerName;
        Seeds = seeds.Length > 0
            ? seeds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : new[] { $"{host}:{port}" };
        User = user;
        _password = password;
        Consistency = consistency;
        if (timeout.HasValue) Timeout = timeout.Value;
    }

    /// <summary>Construct from named arguments.</summary>
    public SkaidbConnection(
        string host = "localhost",
        int port = 7000,
        string user = "anonymous",
        string password = "",
        SkaidbConsistency consistency = SkaidbConsistency.Quorum,
        string database = "",
        bool tls = false,
        string tlsCa = "",
        bool tlsInsecure = false,
        string tlsServerName = "skaidb")
    {
        Host = host;
        Port = port;
        User = user;
        _password = password;
        Consistency = consistency;
        Database = database;
        Tls = tls || tlsCa.Length > 0 || tlsInsecure;
        TlsCa = tlsCa;
        TlsInsecure = tlsInsecure;
        TlsServerName = tlsServerName;
        Seeds = new[] { $"{host}:{port}" };
    }

    /// <summary>
    /// Endpoints to try, in shuffled order, until one connects. skaidb is
    /// leaderless, so any node serves — there is no primary to discover.
    /// </summary>
    public IReadOnlyList<string> Seeds { get; } = Array.Empty<string>();

    /// <summary>Session database, selected with USE right after connecting.</summary>
    public string Database { get; } = "";
    /// <summary>Whether to wrap the connection in TLS.</summary>
    public bool Tls { get; }
    /// <summary>PEM CA bundle used to verify the server certificate.</summary>
    public string TlsCa { get; } = "";
    /// <summary>Encrypt without verifying the certificate. Development only.</summary>
    public bool TlsInsecure { get; }
    /// <summary>SNI name; must match a SAN on the server certificate.</summary>
    public string TlsServerName { get; } = "skaidb";

    private static SkaidbConsistency ParseConsistency(string value)
    {
        switch (value.Trim().ToUpperInvariant())
        {
            case "ONE":
            case "0":
                return SkaidbConsistency.One;
            case "QUORUM":
            case "1":
                return SkaidbConsistency.Quorum;
            case "ALL":
            case "2":
                return SkaidbConsistency.All;
            default:
                throw new SkaidbException($"invalid consistency '{value}'");
        }
    }

    /// <summary>True once <see cref="Open"/> has completed successfully.</summary>
    public bool IsOpen => _open;

    /// <summary>Connect and run the SCRAM-SHA-256 handshake. Idempotent.</summary>
    public void Open()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (_open) return;

        try
        {
            // Try each seed until one connects; a dead node must not swallow
            // the attempt.
            var order = new List<string>(Seeds);
            var rng = new Random();
            for (int i = order.Count - 1; i > 0; i--)
            {
                int j = rng.Next(i + 1);
                (order[i], order[j]) = (order[j], order[i]);
            }
            TcpClient? tcp = null;
            Exception? last = null;
            foreach (var ep in order)
            {
                int c = ep.LastIndexOf(':');
                string h = c > 0 ? ep.Substring(0, c) : ep;
                int p = c > 0 ? int.Parse(ep.Substring(c + 1), CultureInfo.InvariantCulture) : 7000;
                var candidate = new TcpClient { NoDelay = true };
                candidate.SendTimeout = (int)Timeout.TotalMilliseconds;
                candidate.ReceiveTimeout = (int)Timeout.TotalMilliseconds;
                try
                {
                    var t = candidate.ConnectAsync(h, p);
                    if (!t.Wait(Timeout)) throw new SkaidbException($"connect to {ep} timed out");
                    tcp = candidate;
                    break;
                }
                catch (Exception e)
                {
                    last = e;
                    candidate.Dispose();
                }
            }
            if (tcp is null)
            {
                string where = $"no reachable endpoint in {string.Join(", ", order)}";
                throw last is null
                    ? new SkaidbException(where)
                    : new SkaidbException($"{where}: {last.Message}", last);
            }
            tcp.NoDelay = true;
            var stream = tcp.GetStream();
            stream.ReadTimeout = (int)Timeout.TotalMilliseconds;
            stream.WriteTimeout = (int)Timeout.TotalMilliseconds;

            _tcp = tcp;
            _stream = Tls ? TlsWrap(stream) : stream;
            Handshake(User, _password);
            _open = true;
            SendHello();
            // USE is per-connection session state, so it runs on every open.
            if (Database.Length > 0)
            {
                using var use = CreateCommand();
                use.CommandText = "USE \"" + Database.Replace("\"", "\"\"") + "\"";
                use.ExecuteNonQuery();
            }
        }
        catch (SkaidbException)
        {
            CleanupSocket();
            throw;
        }
        catch (Exception e)
        {
            CleanupSocket();
            throw new SkaidbException($"connect failed: {e.Message}", e);
        }
    }

    /// <summary>
    /// Upgrade a connected stream to TLS. The SNI/verified name must match a
    /// SAN on the server certificate — skaidb's own certs carry DNS:skaidb,
    /// which is usually NOT the address dialled, hence the separate knob.
    /// </summary>
    private Stream TlsWrap(NetworkStream raw)
    {
        RemoteCertificateValidationCallback? cb = null;
        X509Certificate2Collection? extra = null;
        if (TlsInsecure)
        {
            // Encrypts, but authenticates nothing: a man in the middle can
            // present any certificate. Development only.
            cb = (_, _, _, _) => true;
        }
        else if (TlsCa.Length > 0)
        {
            extra = new X509Certificate2Collection();
            extra.ImportFromPemFile(TlsCa);
            cb = (_, cert, chain, errors) =>
            {
                if (errors == SslPolicyErrors.None) return true;
                if (cert is null) return false;
                // Verify against the supplied CA bundle rather than the
                // machine store, which will not contain a private CA.
                var v = new X509Chain();
                v.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                v.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                foreach (var c in extra) v.ChainPolicy.CustomTrustStore.Add(c);
                return v.Build(new X509Certificate2(cert));
            };
        }
        var ssl = new SslStream(raw, leaveInnerStreamOpen: false, cb);
        ssl.AuthenticateAsClient(TlsServerName);
        return ssl;
    }

    private void CleanupSocket()
    {
        try { _stream?.Dispose(); } catch { /* ignore */ }
        try { _tcp?.Dispose(); } catch { /* ignore */ }
        _stream = null;
        _tcp = null;
    }

    /// <summary>Create a command bound to this connection.</summary>
    public SkaidbCommand CreateCommand() => new SkaidbCommand(this);

    /// <summary>One change captured by a stream.</summary>
    public sealed record StreamEvent(string Id, string Op, object? Key, object? Ts, object? Doc);

    /// <summary>
    /// Yield a stream's events as they arrive, blocking until the enumerator
    /// is abandoned.
    /// </summary>
    /// <remarks>
    /// A dependency-free helper over the stream's log: it pages the log with
    /// the keyset cursor. <c>Id</c> is the position — keep the last one and
    /// pass it as <paramref name="after"/> to resume exactly where you
    /// stopped, across restarts.
    ///
    /// This polls; for push delivery subscribe to <c>$stream/&lt;db&gt;/&lt;name&gt;</c>
    /// with any MQTT client instead. The events are identical.
    /// </remarks>
    public IEnumerable<StreamEvent> Subscribe(string stream, string? after = null, int pollMs = 500)
    {
        string log = "_stream_" + stream;
        string? cur = after;
        while (true)
        {
            int n = 0;
            using (var cmd = CreateCommand())
            {
                if (cur is null)
                {
                    cmd.CommandText = $"SELECT id, op, k, ts, doc FROM {log} ORDER BY id LIMIT 500";
                }
                else
                {
                    cmd.CommandText =
                        $"SELECT id, op, k, ts, doc FROM {log} WHERE id > ? ORDER BY id LIMIT 500";
                    cmd.Parameters.Add(cur);
                }
                using var r = cmd.ExecuteReader();
                var batch = new List<StreamEvent>();
                while (r.Read())
                {
                    string id = r.GetString(0);
                    cur = id;
                    n++;
                    batch.Add(new StreamEvent(id, r.GetString(1), r.GetValue(2), r.GetValue(3), r.GetValue(4)));
                }
                foreach (var ev in batch) yield return ev;
            }
            if (n == 0) Thread.Sleep(pollMs);
        }
    }

    /// <summary>
    /// False once disposed, once a transport error broke the socket, or while a
    /// <see cref="Stream"/> is still in flight. The last case is what keeps a
    /// pool honest: a connection whose stream was abandoned without being
    /// disposed still owes unread frames, and handing it out would desync
    /// whoever got it next.
    /// </summary>
    public bool IsUsable => !_disposed && _open && !_broken && !_streaming;

    public void Close() => Dispose();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _open = false;
        CleanupSocket();
    }

    /// <summary>
    /// Re-dial if the transport died since the last statement, BEFORE anything
    /// is prepared on it.
    /// </summary>
    /// <remarks>
    /// The prepared-statement cache MUST be cleared: an id is only valid on
    /// the connection that created it, so carrying one across a reconnect
    /// would run a different statement (or fail obscurely).
    /// </remarks>
    internal void EnsureLive()
    {
        // Re-dialling would swap the socket out from under an in-flight
        // stream, so refuse before the caller loses frames. Unlocked
        // first: the lock is held for the whole of each frame read, so
        // waiting for it means waiting out a socket timeout to be told
        // about a mistake the flag already knows about.
        ThrowIfStreamingUnlocked();
        lock (_lock) ThrowIfStreamingLocked();
        if (!_broken) return;
        _prepared.Clear();
        CleanupSocket();
        // Open() is idempotent and returns early while _open is set, so the
        // flag must drop or the re-dial would silently keep the dead socket.
        _open = false;
        // Cleared BEFORE dialling: Open() issues USE, which runs a statement
        // and would otherwise re-enter this method forever.
        _broken = false;
        try
        {
            Open();
        }
        catch
        {
            _broken = true;   // still down; the next statement retries
            throw;
        }
    }

    /// <summary>
    /// Refuse a request while a <see cref="Stream"/> holds the connection,
    /// WITHOUT waiting for the lock.
    ///
    /// The locked check below is the authority, but reaching it means
    /// blocking on the Monitor until the streaming thread is between
    /// frames — so a caller who made a threading mistake waits out a socket
    /// timeout before learning what they did wrong. `_streaming` is
    /// volatile precisely so it can be read here first. A false negative
    /// (the flag set just after this reads it) is harmless: the locked
    /// check still catches it.
    /// </summary>
    private void ThrowIfStreamingUnlocked()
    {
        if (_streaming)
            throw new SkaidbException(
                "connection is busy streaming: the protocol allows no other request until the "
                + "stream ends, so finish or dispose the Stream() enumerator (or use a second "
                + "connection) before running this statement");
    }

    /// <summary>
    /// Refuse a request while a <see cref="Stream"/> holds the connection.
    /// Call with <see cref="_lock"/> held, so the check and the WriteFrame it
    /// guards cannot be split by the streaming thread.
    /// </summary>
    private void ThrowIfStreamingLocked()
    {
        if (_streaming)
            throw new SkaidbException(
                "connection is busy streaming: the protocol allows no other request until the "
                + "stream ends, so finish or dispose the Stream() enumerator (or use a second "
                + "connection) before running this statement");
    }

    // ---- framing -----------------------------------------------------------

    private void WriteFrame(byte[] payload)
    {
        if (_stream is null) throw new SkaidbException("connection is not open");
        Span<byte> head = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(head, (uint)payload.Length); // length is BIG-endian
        try
        {
            _stream.Write(head);
            _stream.Write(payload, 0, payload.Length);
            _stream.Flush();
        }
        catch (IOException e)
        {
            _broken = true;
            throw new SkaidbException($"write failed: {e.Message}", e);
        }
    }

    private byte[] ReadFrame()
    {
        byte[] head = ReadExact(4);
        uint length = BinaryPrimitives.ReadUInt32BigEndian(head); // length is BIG-endian
        if (length > 64 * 1024 * 1024)
            throw new SkaidbException($"frame too large: {length} bytes");
        return ReadExact((int)length);
    }

    // NetworkStream.Read can return short; loop until N bytes are read.
    private byte[] ReadExact(int n)
    {
        if (_stream is null) throw new SkaidbException("connection is not open");
        var buf = new byte[n];
        int got = 0;
        while (got < n)
        {
            int read;
            try
            {
                read = _stream.Read(buf, got, n - got);
            }
            catch (IOException e)
            {
                // The statement may already have executed, so it is NOT
                // retried here — an ambiguous write must never repeat. The
                // connection is marked broken and the NEXT statement re-dials.
                _broken = true;
                throw new SkaidbException($"read failed: {e.Message}", e);
            }
            if (read <= 0)
            {
                _broken = true;
                throw new SkaidbException("connection closed by server");
            }
            got += read;
        }
        return buf;
    }

    // ---- handshake ---------------------------------------------------------

    /// <summary>
    /// Best-effort self-identification: fills the server's drivers table
    /// client_name/client_version. An old server answers the unknown opcode
    /// with an error frame, which is ignored — identity is telemetry, never
    /// load-bearing.
    /// </summary>
    private void SendHello()
    {
        try
        {
            var name = System.Text.Encoding.UTF8.GetBytes("dotnet");
            var ver = System.Text.Encoding.UTF8.GetBytes("0.1.0");
            var w = new BinWriter();
            w.U8(8);
            w.U32((uint)name.Length);
            w.Raw(name);
            w.U32((uint)ver.Length);
            w.Raw(ver);
            WriteFrame(w.ToArray());
            ReadFrame();
        }
        catch (Exception)
        {
            // telemetry only
        }
    }

    private void Handshake(string user, string password)
    {
        int counter = Interlocked.Increment(ref _nonceCounter);
        string clientNonce = $"cs{Environment.CurrentManagedThreadId}.{counter}.{Environment.TickCount64}";

        // AuthStart: u8 10, str username, str client_nonce
        {
            var w = new BinWriter();
            w.U8(10);
            w.Str(user);
            w.Str(clientNonce);
            WriteFrame(w.ToArray());
        }

        // AuthChallenge: u8 11, blob salt, u32 iterations, str server_nonce
        byte[] salt;
        uint iterations;
        string serverNonce;
        {
            var r = new BinReader(ReadFrame());
            if (r.U8() != 11) throw new SkaidbException("bad handshake challenge");
            salt = r.Blob();
            iterations = r.U32();
            serverNonce = r.Text();
        }

        string saltHex = ToLowerHex(salt);
        // auth_message = user \0 client_nonce \0 server_nonce \0 salt_hex \0 iterations
        string authMessageStr = string.Join(
            "\0",
            user, clientNonce, serverNonce, saltHex, iterations.ToString(CultureInfo.InvariantCulture));
        byte[] authMessage = Encoding.UTF8.GetBytes(authMessageStr);

        var (proof, expectedServerSig) = ScramProof(password, salt, (int)iterations, authMessage);

        // AuthFinish: u8 12, 32 raw proof bytes (NOT length-prefixed)
        {
            var payload = new byte[1 + 32];
            payload[0] = 12;
            Array.Copy(proof, 0, payload, 1, 32);
            WriteFrame(payload);
        }

        // AuthOutcome: u8 13, u8 ok; if ok: 32 raw server_sig; else str reason
        {
            var r = new BinReader(ReadFrame());
            if (r.U8() != 13) throw new SkaidbException("bad handshake outcome");
            if (r.U8() == 1)
            {
                byte[] serverSig = r.Take(32);
                if (password.Length > 0 && !FixedTimeEquals(serverSig, expectedServerSig))
                    throw new SkaidbException("server signature mismatch (mutual auth failed)");
            }
            else
            {
                throw new SkaidbException($"authentication denied: {r.Text()}");
            }
        }
    }

    private static (byte[] proof, byte[] serverSig) ScramProof(
        string password, byte[] salt, int iterations, byte[] authMessage)
    {
        byte[] pw = Encoding.UTF8.GetBytes(password);
        byte[] salted;
        using (var pbkdf2 = new Rfc2898DeriveBytes(pw, salt, iterations, HashAlgorithmName.SHA256))
        {
            salted = pbkdf2.GetBytes(32); // dkLen = 32
        }

        byte[] clientKey = HmacSha256(salted, Encoding.ASCII.GetBytes("Client Key"));
        byte[] storedKey = Sha256(clientKey);
        byte[] clientSig = HmacSha256(storedKey, authMessage);

        var proof = new byte[32];
        for (int i = 0; i < 32; i++) proof[i] = (byte)(clientKey[i] ^ clientSig[i]);

        byte[] serverKey = HmacSha256(salted, Encoding.ASCII.GetBytes("Server Key"));
        byte[] serverSig = HmacSha256(serverKey, authMessage);
        return (proof, serverSig);
    }

    private static byte[] HmacSha256(byte[] key, byte[] message)
    {
        using var h = new HMACSHA256(key); // key first, then message
        return h.ComputeHash(message);
    }

    private static byte[] Sha256(byte[] data)
    {
        using var h = SHA256.Create();
        return h.ComputeHash(data);
    }

    private static bool FixedTimeEquals(byte[] a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        int diff = 0;
        for (int i = 0; i < a.Length; i++) diff |= a[i] ^ b[i];
        return diff == 0;
    }

    private static string ToLowerHex(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    // ---- query (internal) --------------------------------------------------

    internal QueryResult Query(string sql, SkaidbConsistency consistency)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (!_open) throw new SkaidbException("connection is not open");

        byte[] sqlBytes = Encoding.UTF8.GetBytes(sql);
        var w = new BinWriter();
        w.U8(1);                  // OP_QUERY
        w.U8((byte)consistency);  // consistency
        w.U32((uint)sqlBytes.Length); // sql_len, LITTLE-endian
        w.Raw(sqlBytes);
        return Roundtrip(w.ToArray());
    }

    /// <summary>
    /// Stream a result set: rows arrive a chunk at a time instead of the
    /// whole set being materialised. For exports and large scans.
    /// <code>
    /// foreach (var row in conn.Stream("SELECT ...")) { ... }
    /// </code>
    /// Takes no parameters — the opcode carries SQL text.
    /// </summary>
    /// <remarks>
    /// <para>The connection is busy for the whole stream: every other
    /// statement on it throws until the stream ends. Ending it means running
    /// the enumeration out, or disposing the enumerator — which
    /// <c>foreach</c> does on its way out, <c>break</c> and exceptions
    /// included. Disposal reads and discards the frames the server still
    /// owes, up to a bounded number of them; past that the connection is
    /// marked broken and the next statement re-dials, rather than the caller
    /// paying to receive an export they walked away from.</para>
    /// <para>An enumerator that is neither finished nor disposed — a
    /// hand-rolled <c>GetEnumerator()</c> that is simply dropped — claims the
    /// connection for good, because nothing in .NET runs an iterator's
    /// <c>finally</c> at collection time. That connection reports
    /// <see cref="IsUsable"/> false, so a pool discards it instead of handing
    /// out a socket with rows still queued on it.</para>
    /// <para>Do not return the enumerable out of
    /// <see cref="SkaidbConnectionPool.WithConnection"/>: it is lazy, and the
    /// connection would go back to the pool before the first row is read.
    /// Enumerate inside the callback.</para>
    /// </remarks>
    public IEnumerable<object?[]> Stream(string sql, SkaidbConsistency? consistency = null)
    {
        EnsureLive();
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (!_open) throw new SkaidbException("connection is not open");

        byte[] sqlBytes = Encoding.UTF8.GetBytes(sql);
        var w = new BinWriter();
        w.U8(5);                                    // OP_QUERY_STREAM
        w.U8((byte)(consistency ?? Consistency));
        w.U32((uint)sqlBytes.Length);
        w.Raw(sqlBytes);

        // A busy flag, not a held lock, is what serialises the exchange.
        // Holding _lock for the stream's duration is not available to an
        // iterator: it yields control between frames, so the Monitor would be
        // held across arbitrary caller code (any other thread touching the
        // connection blocks for as long as the loop body feels like running)
        // and would be released on whichever thread happened to call
        // MoveNext last. The flag instead makes a concurrent statement fail
        // loudly; _lock keeps its old job of guarding one frame's bytes.
        lock (_lock)
        {
            ThrowIfStreamingLocked();
            _streaming = true;
        }

        bool live = false;   // true while the server still owes frames
        try
        {
            BinReader first;
            lock (_lock)
            {
                WriteFrame(w.ToArray());
                first = new BinReader(ReadFrame());
            }
            byte tag = first.U8();
            if (tag == 3)
            {
                string msg = first.Text();
                throw new SkaidbException(msg.Contains("unknown opcode")
                    ? $"server does not support streaming: {msg}" : msg);
            }
            if (tag == 1 || tag == 2) yield break;      // not row-producing
            if (tag != 5)
            {
                // The server is not answering the stream contract, so there is
                // no telling whether more frames follow; draining could block
                // on one that never comes. Break the socket instead.
                _broken = true;
                throw new SkaidbException($"unexpected response tag {tag} to stream request");
            }
            uint ncols = first.U32();
            var columns = new string[ncols];
            for (int i = 0; i < ncols; i++) columns[i] = first.Text();
            StreamColumns = columns;

            live = true;
            while (live)
            {
                BinReader r;
                lock (_lock) { r = new BinReader(ReadFrame()); }
                byte t = r.U8();
                if (t == 6)
                {
                    uint n = r.U32();
                    for (int i = 0; i < n; i++)
                    {
                        uint ncells = r.U32();
                        var row = new object?[ncells];
                        for (int c = 0; c < ncells; c++)
                            row[c] = DecodeValue(new BinReader(r.Blob()));
                        yield return row;
                    }
                }
                else if (t == 7) { live = false; }
                else if (t == 3) { live = false; throw new SkaidbException(r.Text()); }
                else
                {
                    // As above: an out-of-contract tag says nothing about what
                    // still follows, so do not wait around draining for it.
                    live = false;
                    _broken = true;
                    throw new SkaidbException($"unexpected frame tag {t} in stream");
                }
            }
        }
        finally
        {
            // Abandoned early — break, exception, or an explicit Dispose of
            // the enumerator. Read off what the server still owes, so the next
            // statement on this connection does not decode a leftover
            // RowsChunk as its own reply.
            int budget = StreamDrainFrames;
            while (live)
            {
                if (budget-- == 0) { _broken = true; break; }
                try
                {
                    BinReader r;
                    lock (_lock) { r = new BinReader(ReadFrame()); }
                    byte t = r.U8();
                    if (t == 7 || t == 3) live = false;
                    else if (t != 6) { _broken = true; break; }
                }
                catch (Exception)
                {
                    // Whatever stopped the drain leaves the socket at an
                    // unknown offset, and this runs inside a finally: throwing
                    // here would bury the caller's own exception, or turn a
                    // plain `break` into one. Break the connection and let
                    // EnsureLive() re-dial on the next statement.
                    _broken = true;
                    break;
                }
            }
            // Released last: until this drops, IsUsable is false and every
            // other statement on the connection is refused.
            lock (_lock) { _streaming = false; }
        }
    }

    /// <summary>Column names of the most recent <see cref="Stream"/> call.</summary>
    public IReadOnlyList<string> StreamColumns { get; private set; } = Array.Empty<string>();

    /// <summary>
    /// Prepare <paramref name="sql"/> on the SERVER, returning
    /// (statementId, paramCount). Cached per connection — a prepared id is
    /// only meaningful on the connection that created it. Throws
    /// <see cref="UnpreparableException"/> for statement kinds the server
    /// declines (DDL, session statements).
    /// </summary>
    internal (uint Id, int Params) PrepareServer(string sql)
    {
        EnsureLive();
        if (_prepared.TryGetValue(sql, out var hit)) return hit;
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (!_open) throw new SkaidbException("connection is not open");

        byte[] sqlBytes = Encoding.UTF8.GetBytes(sql);
        var w = new BinWriter();
        w.U8(2);                          // OP_PREPARE
        w.U32((uint)sqlBytes.Length);
        w.Raw(sqlBytes);

        BinReader r;
        // Unlocked first so a threading mistake is reported now rather
        // than after the streaming thread's current frame read.
        ThrowIfStreamingUnlocked();
        lock (_lock)
        {
            ThrowIfStreamingLocked();
            WriteFrame(w.ToArray());
            r = new BinReader(ReadFrame());
        }
        byte tag = r.U8();
        if (tag == 4)
        {
            uint id = r.U32();
            int n = r.U16();
            var v = (id, n);
            if (_prepared.Count < 240) _prepared[sql] = v;
            return v;
        }
        if (tag == 3) throw new UnpreparableException(r.Text());
        throw new SkaidbException($"unexpected prepare response tag {tag}");
    }

    /// <summary>
    /// Execute a prepared statement with TYPED parameters — the only way to
    /// send an array or a document, neither of which has a SQL literal form.
    /// </summary>
    internal QueryResult ExecutePrepared(uint id, IReadOnlyList<object?> parameters,
                                         SkaidbConsistency consistency)
    {
        var w = new BinWriter();
        w.U8(3);                          // OP_EXECUTE
        w.U8((byte)consistency);
        w.U32(id);
        w.U16((ushort)parameters.Count);
        foreach (var p in parameters)
        {
            byte[] v = EncodeValue(p);
            w.U32((uint)v.Length);
            w.Raw(v);
        }
        return Roundtrip(w.ToArray());
    }

    /// <summary>
    /// Execute a prepared statement once per row in ONE round-trip. Rows
    /// autocommit individually: a failure names the row and earlier rows stay
    /// applied, so the statement must be idempotent.
    /// </summary>
    internal QueryResult ExecuteBatch(uint id, IReadOnlyList<IReadOnlyList<object?>> rows,
                                      SkaidbConsistency consistency)
    {
        var w = new BinWriter();
        w.U8(7);                          // OP_EXECUTE_BATCH
        w.U8((byte)consistency);
        w.U32(id);
        w.U32((uint)rows.Count);
        foreach (var parameters in rows)
        {
            w.U16((ushort)parameters.Count);
            foreach (var p in parameters)
            {
                byte[] v = EncodeValue(p);
                w.U32((uint)v.Length);
                w.Raw(v);
            }
        }
        return Roundtrip(w.ToArray());
    }

    /// <summary>
    /// Encode a value as a TYPED skaidb value (tag + payload) — the inverse
    /// of DecodeValue. Enumerables become Array, dictionaries become
    /// Document, nested arbitrarily.
    /// </summary>
    internal static byte[] EncodeValue(object? v)
    {
        var w = new BinWriter();
        EncodeInto(v, w);
        return w.ToArray();
    }

    private static void EncodeInto(object? v, BinWriter w)
    {
        switch (v)
        {
            case null:
            case DBNull:
                w.U8(0);
                return;
            case bool b:
                w.U8(1); w.U8((byte)(b ? 1 : 0));
                return;
            case sbyte or byte or short or ushort or int or uint or long:
                w.U8(2); w.I64(Convert.ToInt64(v, CultureInfo.InvariantCulture));
                return;
            case float or double:
            {
                double d = Convert.ToDouble(v, CultureInfo.InvariantCulture);
                if (double.IsNaN(d) || double.IsInfinity(d))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                w.U8(3); w.I64(BitConverter.DoubleToInt64Bits(d));
                return;
            }
            case string str:
            {
                byte[] b = Encoding.UTF8.GetBytes(str);
                w.U8(5); w.U32((uint)b.Length); w.Raw(b);
                return;
            }
            case byte[] raw:
                w.U8(6); w.U32((uint)raw.Length); w.Raw(raw);
                return;
            case Guid g:
            {
                // Canonical big-endian byte order, matching the decoder.
                byte[] le = g.ToByteArray();
                byte[] be = new byte[16];
                be[0] = le[3]; be[1] = le[2]; be[2] = le[1]; be[3] = le[0];
                be[4] = le[5]; be[5] = le[4];
                be[6] = le[7]; be[7] = le[6];
                Array.Copy(le, 8, be, 8, 8);
                w.U8(7); w.Raw(be);
                return;
            }
            case DateTimeOffset dto:
                w.U8(8); w.I64(dto.ToUnixTimeMilliseconds());
                return;
            case DateTime dt:
                w.U8(8); w.I64(new DateTimeOffset(dt.ToUniversalTime()).ToUnixTimeMilliseconds());
                return;
            case System.Collections.IDictionary map:
            {
                w.U8(10); w.U32((uint)map.Count);
                foreach (System.Collections.DictionaryEntry e in map)
                {
                    if (e.Key is not string ks)
                        throw new SkaidbException("document keys must be strings");
                    byte[] kb = Encoding.UTF8.GetBytes(ks);
                    w.U32((uint)kb.Length); w.Raw(kb);
                    EncodeInto(e.Value, w);
                }
                return;
            }
            case System.Collections.IEnumerable seq:
            {
                var items = new List<object?>();
                foreach (var item in seq) items.Add(item);
                w.U8(9); w.U32((uint)items.Count);
                foreach (var item in items) EncodeInto(item, w);
                return;
            }
        }
        throw new SkaidbException($"cannot bind value of type {v.GetType().Name}");
    }

    private QueryResult Roundtrip(byte[] request)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (!_open) throw new SkaidbException("connection is not open");

        BinReader r;
        // Unlocked first so a threading mistake is reported now rather
        // than after the streaming thread's current frame read.
        ThrowIfStreamingUnlocked();
        lock (_lock)
        {
            ThrowIfStreamingLocked();
            WriteFrame(request);
            r = new BinReader(ReadFrame());
        }

        byte tag = r.U8();
        switch (tag)
        {
            case 0: // Rows
            {
                uint ncols = r.U32();
                var columns = new string[ncols];
                for (int i = 0; i < ncols; i++) columns[i] = r.Text();
                uint nrows = r.U32();
                var rows = new List<object?[]>((int)nrows);
                for (int ri = 0; ri < nrows; ri++)
                {
                    uint ncells = r.U32();
                    var row = new object?[ncells];
                    for (int ci = 0; ci < ncells; ci++)
                    {
                        // each cell is a length-prefixed self-describing Value
                        byte[] cellBytes = r.Blob();
                        var cr = new BinReader(cellBytes);
                        row[ci] = DecodeValue(cr);
                    }
                    rows.Add(row);
                }
                return new QueryResult(QueryResultKind.Rows, columns, rows, 0);
            }
            case 8: // ResultSets: a CALL whose body EMITted
            {
                uint nsets = r.U32();
                var sets = new List<QueryResult>((int)nsets);
                for (int s = 0; s < nsets; s++)
                {
                    uint ncols = r.U32();
                    var columns = new string[ncols];
                    for (int i = 0; i < ncols; i++) columns[i] = r.Text();
                    uint nrows = r.U32();
                    var rows = new List<object?[]>((int)nrows);
                    for (int ri = 0; ri < nrows; ri++)
                    {
                        uint ncells = r.U32();
                        var row = new object?[ncells];
                        for (int ci = 0; ci < ncells; ci++)
                            row[ci] = DecodeValue(new BinReader(r.Blob()));
                        rows.Add(row);
                    }
                    sets.Add(new QueryResult(QueryResultKind.Rows, columns, rows, 0));
                }
                if (sets.Count == 0)
                    return new QueryResult(QueryResultKind.Rows, new string[0], new List<object?[]>(), 0);
                // The first set is current; the reader's NextResult() walks the rest.
                var first = sets[0];
                return new QueryResult(QueryResultKind.Rows, first.Columns, first.Rows, 0)
                {
                    MoreSets = sets.GetRange(1, sets.Count - 1)
                };
            }
            case 1: // Mutation
                return new QueryResult(QueryResultKind.Mutation, null, null, r.U64());
            case 2: // Ddl
                return new QueryResult(QueryResultKind.Ddl, null, null, 0);
            case 3: // Error
                throw new SkaidbException(r.Text());
            default:
                throw new SkaidbException($"unknown response tag {tag}");
        }
    }

    // ---- value codec (§4) --------------------------------------------------

    private const byte TagNull = 0;
    private const byte TagBool = 1;
    private const byte TagInt = 2;
    private const byte TagFloat = 3;
    private const byte TagDecimal = 4;
    private const byte TagString = 5;
    private const byte TagBytes = 6;
    private const byte TagUuid = 7;
    private const byte TagTimestamp = 8;
    private const byte TagArray = 9;
    private const byte TagDocument = 10;

    internal static object? DecodeValue(BinReader r)
    {
        byte tag = r.U8();
        switch (tag)
        {
            case TagNull:
                return null;
            case TagBool:
                return r.U8() != 0;
            case TagInt:
                return r.I64();
            case TagFloat:
                return BitConverter.Int64BitsToDouble(r.I64());
            case TagDecimal:
            {
                byte[] mantissaBytes = r.Take(16); // i128 LE
                uint scale = r.U32();
                return DecodeDecimal(mantissaBytes, scale);
            }
            case TagString:
                return r.Text();
            case TagBytes:
                return r.Blob();
            case TagUuid:
                return GuidFromRfc4122(r.Take(16));
            case TagTimestamp:
                return DateTimeOffset.FromUnixTimeMilliseconds(r.I64()); // UTC
            case TagArray:
            {
                uint count = r.U32();
                var arr = new object?[count];
                for (int i = 0; i < count; i++) arr[i] = DecodeValue(r);
                return arr;
            }
            case TagDocument:
            {
                uint count = r.U32();
                // Preserve insertion order. Dictionary preserves insertion order
                // for enumeration as long as nothing is removed.
                var doc = new Dictionary<string, object?>((int)count);
                for (int i = 0; i < count; i++)
                {
                    string key = r.Text();
                    doc[key] = DecodeValue(r);
                }
                return doc;
            }
            default:
                throw new SkaidbException($"unknown value tag {tag}");
        }
    }

    // Decimal: value = mantissa / 10^scale. System.Decimal when it fits, else a string.
    private static object DecodeDecimal(byte[] mantissaLeBytes, uint scale)
    {
        // i128 little-endian, signed.
        var mantissa = new BigInteger(mantissaLeBytes, isUnsigned: false, isBigEndian: false);

        // System.Decimal holds a 96-bit unsigned integer with scale 0..28.
        // Try to use it when both the magnitude and the scale fit; otherwise
        // fall back to a lossless decimal string.
        if (scale <= 28)
        {
            BigInteger mag = BigInteger.Abs(mantissa);
            // 96-bit unsigned max.
            BigInteger max96 = (BigInteger.One << 96) - 1;
            if (mag <= max96)
            {
                // Build the int parts of the 96-bit magnitude.
                uint lo = (uint)(mag & 0xFFFFFFFF);
                uint mid = (uint)((mag >> 32) & 0xFFFFFFFF);
                uint hi = (uint)((mag >> 64) & 0xFFFFFFFF);
                return new decimal((int)lo, (int)mid, (int)hi, mantissa.Sign < 0, (byte)scale);
            }
        }

        // Fallback: format mantissa/10^scale as a plain decimal string.
        return FormatBigDecimalString(mantissa, scale);
    }

    private static string FormatBigDecimalString(BigInteger mantissa, uint scale)
    {
        if (scale == 0) return mantissa.ToString(CultureInfo.InvariantCulture);

        bool negative = mantissa.Sign < 0;
        string digits = BigInteger.Abs(mantissa).ToString(CultureInfo.InvariantCulture);
        if (digits.Length <= scale)
            digits = new string('0', (int)scale - digits.Length + 1) + digits;

        int pointPos = digits.Length - (int)scale;
        string intPart = digits.Substring(0, pointPos);
        string fracPart = digits.Substring(pointPos);
        string result = intPart + "." + fracPart;
        return negative ? "-" + result : result;
    }

    // The 16 bytes are RFC-4122 / big-endian order. Guid(byte[]) interprets the
    // first 3 fields as little-endian, so build the Guid from its canonical
    // 8-4-4-4-12 string form to avoid byte reordering.
    private static Guid GuidFromRfc4122(byte[] b)
    {
        var sb = new StringBuilder(36);
        AppendHex(sb, b, 0, 4);
        sb.Append('-');
        AppendHex(sb, b, 4, 2);
        sb.Append('-');
        AppendHex(sb, b, 6, 2);
        sb.Append('-');
        AppendHex(sb, b, 8, 2);
        sb.Append('-');
        AppendHex(sb, b, 10, 6);
        return new Guid(sb.ToString());
    }

    private static void AppendHex(StringBuilder sb, byte[] b, int offset, int count)
    {
        for (int i = 0; i < count; i++)
            sb.Append(b[offset + i].ToString("x2", CultureInfo.InvariantCulture));
    }
}

// ---- internal result carrier ----------------------------------------------

internal enum QueryResultKind { Rows, Mutation, Ddl }

internal sealed class QueryResult
{
    public QueryResultKind Kind { get; }
    public string[]? Columns { get; }
    public List<object?[]>? Rows { get; }
    public ulong Affected { get; }
    /// <summary>Further result sets of a multi-set reply (a CALL whose body EMITted).</summary>
    public List<QueryResult> MoreSets { get; set; } = new List<QueryResult>();

    public QueryResult(QueryResultKind kind, string[]? columns, List<object?[]>? rows, ulong affected)
    {
        Kind = kind;
        Columns = columns;
        Rows = rows;
        Affected = affected;
    }
}

/// <summary>
/// A SQL command. Set <see cref="CommandText"/> with `?` placeholders and add
/// values to <see cref="Parameters"/> in order, then execute.
/// </summary>
public sealed class SkaidbCommand : IDisposable
{
    private readonly SkaidbConnection _connection;

    public string CommandText { get; set; } = "";

    /// <summary>Positional parameters, bound to `?` placeholders in order.</summary>
    public List<object?> Parameters { get; } = new();

    /// <summary>Per-command consistency override. Defaults to the connection's.</summary>
    public SkaidbConsistency Consistency { get; set; }

    public SkaidbCommand(SkaidbConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        Consistency = connection.Consistency;
    }

    /// <summary>Execute and return a reader over the result rows. One reader per execution.</summary>
    public SkaidbDataReader ExecuteReader()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Rows)
            return new SkaidbDataReader(result.Columns!, result.Rows!, result.MoreSets);
        // Non-row results expose an empty reader (FieldCount 0, Read() false).
        return new SkaidbDataReader(Array.Empty<string>(), new List<object?[]>());
    }

    /// <summary>Execute a statement and return the number of affected rows (0 for DDL/SELECT).</summary>
    public int ExecuteNonQuery()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Mutation)
        {
            ulong n = result.Affected;
            return n > int.MaxValue ? int.MaxValue : (int)n;
        }
        return 0;
    }

    /// <summary>Execute and return the first column of the first row (or null).</summary>
    public object? ExecuteScalar()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Rows && result.Rows is { Count: > 0 } rows)
        {
            var first = rows[0];
            return first.Length > 0 ? first[0] : null;
        }
        return null;
    }

    private QueryResult Run()
    {
        // Recover a transport that died since the last statement, before
        // anything is prepared on it.
        _connection.EnsureLive();
        // Server-side prepare so parameters travel as TYPED values; arrays
        // and documents have no SQL literal form and cannot be interpolated.
        if (Parameters.Count > 0)
        {
            try
            {
                var (id, n) = _connection.PrepareServer(CommandText);
                if (n != Parameters.Count)
                    throw new SkaidbException(
                        $"statement expects {n} parameters, got {Parameters.Count}");
                return _connection.ExecutePrepared(id, Parameters, Consistency);
            }
            catch (UnpreparableException)
            {
                // Statement kind the server will not prepare: fall back to
                // client-side text binding.
            }
        }
        string sql = ParameterBinder.Bind(CommandText, Parameters);
        return _connection.Query(sql, Consistency);
    }

    /// <summary>
    /// Execute this statement once per row in ONE round-trip. Rows autocommit
    /// individually: a failure names the row and earlier rows stay applied,
    /// so the statement must be idempotent. Returns total affected rows.
    /// </summary>
    public long ExecuteBatch(IReadOnlyList<IReadOnlyList<object?>> rows)
    {
        if (rows.Count == 0) return 0;
        var (id, n) = _connection.PrepareServer(CommandText);
        foreach (var r in rows)
        {
            if (r.Count != n)
                throw new SkaidbException($"batch row expects {n} parameters, got {r.Count}");
        }
        var result = _connection.ExecuteBatch(id, rows, Consistency);
        return result.Kind == QueryResultKind.Mutation ? (long)result.Affected : 0;
    }

    public void Dispose() { /* nothing owned */ }
}

/// <summary>Forward-only reader over a result set. Created by <see cref="SkaidbCommand.ExecuteReader"/>.</summary>
public sealed class SkaidbDataReader : IDisposable
{
    private string[] _columns;
    private List<object?[]> _rows;
    private int _pos = -1;
    private readonly List<QueryResult> _more;

    /// <summary>Advance to the next result set of a multi-set reply (a CALL whose
    /// body EMITted). Returns false when there is none.</summary>
    public bool NextResult()
    {
        if (_more.Count == 0) return false;
        var next = _more[0];
        _more.RemoveAt(0);
        _columns = next.Columns ?? new string[0];
        _rows = next.Rows ?? new List<object?[]>();
        _pos = -1;
        return true;
    }

    internal SkaidbDataReader(string[] columns, List<object?[]> rows, List<QueryResult>? more = null)
    {
        _columns = columns;
        _rows = rows;
        _more = more ?? new List<QueryResult>();
    }

    /// <summary>Number of columns in the result.</summary>
    public int FieldCount => _columns.Length;

    /// <summary>Advance to the next row. Returns false when exhausted.</summary>
    public bool Read()
    {
        if (_pos + 1 >= _rows.Count) { _pos = _rows.Count; return false; }
        _pos++;
        return true;
    }

    private object?[] Current
    {
        get
        {
            if (_pos < 0) throw new SkaidbException("call Read() before accessing fields");
            if (_pos >= _rows.Count) throw new SkaidbException("no current row");
            return _rows[_pos];
        }
    }

    public string GetName(int i) => _columns[i];

    public int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.Ordinal))
                return i;
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new SkaidbException($"no column named '{name}'");
    }

    public bool IsDBNull(int i) => Current[i] is null;

    /// <summary>Raw value; null maps to <see cref="DBNull.Value"/>.</summary>
    public object GetValue(int i) => Current[i] ?? DBNull.Value;

    public bool GetBoolean(int i) => Convert.ToBoolean(NonNull(i), CultureInfo.InvariantCulture);

    public long GetInt64(int i) => Convert.ToInt64(NonNull(i), CultureInfo.InvariantCulture);

    public int GetInt32(int i) => Convert.ToInt32(NonNull(i), CultureInfo.InvariantCulture);

    public double GetDouble(int i) => Convert.ToDouble(NonNull(i), CultureInfo.InvariantCulture);

    public decimal GetDecimal(int i) => Convert.ToDecimal(NonNull(i), CultureInfo.InvariantCulture);

    public string GetString(int i)
    {
        object v = NonNull(i);
        return v as string ?? Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
    }

    public Guid GetGuid(int i)
    {
        object v = NonNull(i);
        return v is Guid g ? g : Guid.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!);
    }

    public DateTimeOffset GetDateTimeOffset(int i) => (DateTimeOffset)NonNull(i);

    public byte[] GetBytes(int i) => (byte[])NonNull(i);

    private object NonNull(int i)
    {
        object? v = Current[i];
        if (v is null) throw new SkaidbException($"column {i} is NULL");
        return v;
    }

    /// <summary>Value by ordinal (DBNull for null).</summary>
    public object this[int i] => GetValue(i);

    /// <summary>Value by column name (DBNull for null).</summary>
    public object this[string column] => GetValue(GetOrdinal(column));

    public void Dispose() { /* nothing owned */ }
}

// ---- client-side parameter binding (§5) ------------------------------------

internal static class ParameterBinder
{
    public static string Bind(string sql, IReadOnlyList<object?> parameters)
    {
        if (parameters is null || parameters.Count == 0)
        {
            if (HasPlaceholderOutsideStrings(sql))
                throw new SkaidbException("query has placeholders but no parameters given");
            return sql;
        }

        var sb = new StringBuilder(sql.Length + 16);
        bool inStr = false;
        int i = 0;
        int n = sql.Length;
        int used = 0;
        while (i < n)
        {
            char ch = sql[i];
            if (inStr)
            {
                sb.Append(ch);
                if (ch == '\'')
                {
                    // A doubled '' is an escaped quote and stays inside the string.
                    if (i + 1 < n && sql[i + 1] == '\'')
                    {
                        sb.Append('\'');
                        i += 2;
                        continue;
                    }
                    inStr = false;
                }
                i++;
                continue;
            }
            if (ch == '\'')
            {
                inStr = true;
                sb.Append(ch);
                i++;
                continue;
            }
            if (ch == '?')
            {
                if (used >= parameters.Count)
                    throw new SkaidbException("more placeholders than parameters");
                sb.Append(Quote(parameters[used]));
                used++;
                i++;
                continue;
            }
            sb.Append(ch);
            i++;
        }
        if (used < parameters.Count)
            throw new SkaidbException("more parameters than placeholders");
        return sb.ToString();
    }

    private static bool HasPlaceholderOutsideStrings(string sql)
    {
        bool inStr = false;
        int i = 0, n = sql.Length;
        while (i < n)
        {
            char ch = sql[i];
            if (inStr)
            {
                if (ch == '\'')
                {
                    if (i + 1 < n && sql[i + 1] == '\'') { i += 2; continue; }
                    inStr = false;
                }
                i++;
                continue;
            }
            if (ch == '\'') { inStr = true; i++; continue; }
            if (ch == '?') return true;
            i++;
        }
        return false;
    }

    private static string Quote(object? arg)
    {
        switch (arg)
        {
            case null:
            case DBNull:
                return "NULL";
            case bool b:
                return b ? "TRUE" : "FALSE";
            case byte v:
                return v.ToString(CultureInfo.InvariantCulture);
            case sbyte v:
                return v.ToString(CultureInfo.InvariantCulture);
            case short v:
                return v.ToString(CultureInfo.InvariantCulture);
            case ushort v:
                return v.ToString(CultureInfo.InvariantCulture);
            case int v:
                return v.ToString(CultureInfo.InvariantCulture);
            case uint v:
                return v.ToString(CultureInfo.InvariantCulture);
            case long v:
                return v.ToString(CultureInfo.InvariantCulture);
            case ulong v:
                return v.ToString(CultureInfo.InvariantCulture);
            case BigInteger v:
                return v.ToString(CultureInfo.InvariantCulture);
            case float f:
                if (float.IsNaN(f) || float.IsInfinity(f))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                return f.ToString("R", CultureInfo.InvariantCulture);
            case double d:
                if (double.IsNaN(d) || double.IsInfinity(d))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                return d.ToString("R", CultureInfo.InvariantCulture);
            case decimal m:
                return m.ToString(CultureInfo.InvariantCulture);
            case string s:
                return "'" + s.Replace("'", "''") + "'";
            case char c:
                return "'" + (c == '\'' ? "''" : c.ToString()) + "'";
            case byte[] bytes:
                return "'" + ToLowerHex(bytes) + "'";
            case Guid g:
                return "'" + g.ToString("D", CultureInfo.InvariantCulture) + "'";
            case DateTimeOffset dto:
                return dto.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
            case DateTime dt:
            {
                var utc = dt.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                    : dt.ToUniversalTime();
                long ms = new DateTimeOffset(utc).ToUnixTimeMilliseconds();
                return ms.ToString(CultureInfo.InvariantCulture);
            }
            default:
                throw new SkaidbException($"cannot bind value of type {arg.GetType().Name}");
        }
    }

    private static string ToLowerHex(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }
}

// ---- binary read/write helpers ---------------------------------------------
// All length/integer fields here are LITTLE-endian (the frame length prefix,
// handled in SkaidbConnection, is the only big-endian field in the protocol).

internal sealed class BinReader
{
    private readonly byte[] _buf;
    private int _pos;

    public BinReader(byte[] buf) { _buf = buf; _pos = 0; }

    public byte[] Take(int n)
    {
        int end = _pos + n;
        if (n < 0 || end > _buf.Length) throw new SkaidbException("truncated server message");
        var slice = new byte[n];
        Array.Copy(_buf, _pos, slice, 0, n);
        _pos = end;
        return slice;
    }

    private ReadOnlySpan<byte> Span(int n)
    {
        int end = _pos + n;
        if (n < 0 || end > _buf.Length) throw new SkaidbException("truncated server message");
        var span = new ReadOnlySpan<byte>(_buf, _pos, n);
        _pos = end;
        return span;
    }

    public byte U8()
    {
        if (_pos >= _buf.Length) throw new SkaidbException("truncated server message");
        return _buf[_pos++];
    }

    public ushort U16() => BinaryPrimitives.ReadUInt16LittleEndian(Span(2));
    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Span(4));
    public long I64() => BinaryPrimitives.ReadInt64LittleEndian(Span(8));
    public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Span(8));

    public byte[] Blob() => Take((int)U32());
    public string Text() => Encoding.UTF8.GetString(Blob());
}

internal sealed class BinWriter
{
    private readonly List<byte> _buf = new();

    public void U8(byte b) => _buf.Add(b);

    public void U16(ushort v)
    {
        Span<byte> tmp = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(tmp, v);
        _buf.AddRange(tmp.ToArray());
    }

    public void I64(long v)
    {
        Span<byte> tmp = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(tmp, v);
        _buf.AddRange(tmp.ToArray());
    }

    public void U32(uint v)
    {
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, v);
        _buf.AddRange(tmp.ToArray());
    }

    public void Raw(byte[] bytes) => _buf.AddRange(bytes);

    public void Str(string s)
    {
        byte[] b = Encoding.UTF8.GetBytes(s);
        U32((uint)b.Length);
        _buf.AddRange(b);
    }

    public byte[] ToArray() => _buf.ToArray();
}
/// <summary>
/// A thread-safe pool of skaidb connections.
/// </summary>
/// <remarks>
/// <para><c>maxsize</c> bounds the connections kept IDLE, not the number
/// checked out: a burst creates extras and the surplus is disposed on return.
/// Connections are built from the same connection string, so pooled ones
/// inherit seed failover, TLS and the session database.</para>
/// <code>
/// using var pool = new SkaidbConnectionPool("Host=h1;Port=7000;Database=app", 8);
/// long n = pool.WithConnection(c => {
///     using var cmd = c.CreateCommand();
///     cmd.CommandText = "SELECT count(*) AS n FROM t";
///     using var r = cmd.ExecuteReader();
///     r.Read();
///     return r.GetInt64(0);
/// });
/// </code>
/// </remarks>
public sealed class SkaidbConnectionPool : IDisposable
{
    private readonly string _connectionString;
    private readonly int _maxsize;
    private readonly Stack<SkaidbConnection> _idle = new();
    private readonly object _gate = new();
    private bool _closed;

    public SkaidbConnectionPool(string connectionString, int maxsize = 10)
    {
        if (maxsize < 1) throw new SkaidbException("maxsize must be >= 1");
        _connectionString = connectionString;
        _maxsize = maxsize;
    }

    /// <summary>Check out a usable connection, reusing an idle one when possible.</summary>
    public SkaidbConnection Acquire()
    {
        while (true)
        {
            SkaidbConnection? c = null;
            lock (_gate)
            {
                if (_closed) throw new SkaidbException("pool is closed");
                if (_idle.Count > 0) c = _idle.Pop();
            }
            if (c is null)
            {
                var fresh = new SkaidbConnection(_connectionString);
                fresh.Open();
                return fresh;
            }
            // A connection the server closed while it sat idle still looks
            // fine locally, so check before handing it out.
            if (c.IsUsable) return c;
            c.Dispose();
        }
    }

    /// <summary>Return a connection, disposing it if broken or the pool is full.</summary>
    public void Release(SkaidbConnection c)
    {
        lock (_gate)
        {
            if (!_closed && c.IsUsable && _idle.Count < _maxsize)
            {
                _idle.Push(c);
                return;
            }
        }
        c.Dispose();
    }

    /// <summary>Run <paramref name="work"/> with a checked-out connection.</summary>
    public T WithConnection<T>(Func<SkaidbConnection, T> work)
    {
        var c = Acquire();
        try
        {
            return work(c);
        }
        finally
        {
            Release(c);
        }
    }

    /// <summary>Close the pool and every idle connection.</summary>
    public void Dispose()
    {
        List<SkaidbConnection> drained;
        lock (_gate)
        {
            _closed = true;
            drained = new List<SkaidbConnection>(_idle);
            _idle.Clear();
        }
        foreach (var c in drained) c.Dispose();
    }
}
