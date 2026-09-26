// skaidb — official C# / .NET driver.
//
// Pure BCL (no NuGet packages). A genuine ADO.NET provider: SkaidbConnection,
// SkaidbCommand, SkaidbParameter, SkaidbDataReader, SkaidbBatch and
// SkaidbFactory derive from System.Data.Common, so the driver works anywhere a
// DbConnection does (DbProviderFactories, Dapper, ...).
//
//     using var conn = new SkaidbConnection("Host=localhost;Port=7000;Username=skaidb;Password=secret");
//     conn.Open();
//     using var cmd = conn.CreateCommand();
//     cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
//     cmd.Parameters.Add(1);
//     cmd.Parameters.Add("Ada");
//     cmd.ExecuteNonQuery();
//
//     cmd = conn.CreateCommand();
//     cmd.CommandText = "SELECT id, name FROM users WHERE id = @id";
//     cmd.Parameters.AddWithValue("@id", 1);
//     using var reader = cmd.ExecuteReader();
//     while (reader.Read())
//         Console.WriteLine($"{reader.GetInt64(0)} {reader.GetString(1)}");
//
// Placeholders: `?` (bound positionally), `@name` (bound by ParameterName) or
// `$1` (bound by position); the driver rewrites the latter two to `?` for the
// server's prepared statements.
// The wire protocol is documented at https://skaidb.org/docs/PROTOCOL.html.

using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Numerics;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Skaidb;

/// <summary>Consistency level for a query (how many replicas must agree).</summary>
public enum SkaidbConsistency : byte
{
    One = 0,
    Quorum = 1,
    All = 2,
}

/// <summary>How <see cref="SkaidbConnection"/> authenticates (connection-string key <c>AuthMechanism</c>).</summary>
public enum SkaidbAuthMechanism : byte
{
    /// <summary>SCRAM-SHA-256 with <c>Username</c>/<c>Password</c> (wire mechanism 0). The default.</summary>
    Scram = 0,
    /// <summary>The TLS client certificate is the login (wire mechanism 2, EXTERNAL).</summary>
    Certificate = 2,
}

/// <summary>
/// Thrown on any driver or server-reported error. A <see cref="DbException"/>,
/// so provider-agnostic code can catch it as one.
/// </summary>
public class SkaidbException : DbException
{
    public SkaidbException(string message) : base(message) { }
    public SkaidbException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Statement kind the server declines to prepare (DDL, session statements).
/// Not an error — the caller falls back to client-side text binding.
/// </summary>
public sealed class UnpreparableException : SkaidbException
{
    public UnpreparableException(string message) : base(message) { }
}

/// <summary>
/// A connection to a skaidb cluster: an ADO.NET <see cref="DbConnection"/>.
/// Construct with a connection string (see <see cref="SkaidbConnectionStringBuilder"/>)
/// or named arguments, then call <see cref="Open"/> (idempotent) to dial a seed
/// and authenticate. <see cref="Close"/> releases the socket and the connection
/// can be opened again; <see cref="IDisposable.Dispose"/> is final.
/// </summary>
public sealed class SkaidbConnection : DbConnection
{
    /// <summary>First host of the <c>Host</c> key (without a port).</summary>
    public string Host { get; private set; } = "localhost";
    /// <summary>Default port for hosts that carry none.</summary>
    public int Port { get; private set; } = 7000;
    /// <summary>The login user.</summary>
    public string User { get; private set; } = "anonymous";
    private string _password = "";
    /// <summary>
    /// Whether a user was given explicitly. Certificate login sends the
    /// username only then: an empty one lets the certificate's Common Name
    /// decide, a non-empty one must equal it.
    /// </summary>
    private bool _userExplicit;
    private string _connectionString = "";

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

    /// <summary>Name the driver reports in its Hello frame (the drivers table's client_name).</summary>
    public const string DriverName = "dotnet";

    /// <summary>
    /// The driver's package version, as the Hello frame reports it: the
    /// assembly's informational version, which the build derives from the
    /// single <c>&lt;Version&gt;</c> in Skaidb.csproj. Never a literal.
    /// </summary>
    public static string DriverVersion { get; } = ComputeDriverVersion();

    private static string ComputeDriverVersion()
    {
        var asm = typeof(SkaidbConnection).Assembly;
        string? info = asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(info))
        {
            // "1.0.0+<sha>" when source-revision stamping is on: keep the version part.
            int plus = info.IndexOf('+');
            return plus >= 0 ? info.Substring(0, plus) : info;
        }
        var v = asm.GetName().Version ?? new Version(0, 0, 0);
        return $"{v.Major}.{v.Minor}.{v.Build}";
    }

    /// <summary>Parse a connection string such as
    /// "Host=db1,db2:7001;Port=7000;Username=skaidb;Password=secret;Consistency=Quorum".
    /// Keys are case-insensitive; see <see cref="SkaidbConnectionStringBuilder"/>
    /// for every key and its aliases.</summary>
    public SkaidbConnection(string connectionString)
    {
        if (connectionString is null) throw new ArgumentNullException(nameof(connectionString));
        Configure(connectionString);
    }

    /// <summary>
    /// A closed connection with every setting at its default; set
    /// <see cref="ConnectionString"/> before <see cref="Open"/>. This is what
    /// <see cref="SkaidbFactory.CreateConnection"/> returns.
    /// </summary>
    public SkaidbConnection()
    {
        Configure("");
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
        var b = new SkaidbConnectionStringBuilder
        {
            Host = host,
            Port = port,
            Username = user,
            Password = password,
            Consistency = consistency,
            Database = database,
            Tls = tls,
            TlsCa = tlsCa,
            TlsInsecure = tlsInsecure,
            TlsServerName = tlsServerName,
        };
        Apply(b);
        _userExplicit = user != "anonymous";
    }

    /// <summary>Parse and apply a connection string; any parse error is a <see cref="SkaidbException"/>.</summary>
    private void Configure(string connectionString)
    {
        SkaidbConnectionStringBuilder b;
        try
        {
            b = new SkaidbConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException e)
        {
            throw new SkaidbException(e.Message, e);
        }
        Apply(b);
    }

    private void Apply(SkaidbConnectionStringBuilder b)
    {
        int port = b.Port;
        var hosts = b.Host.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (hosts.Length == 0) hosts = new[] { "localhost" };
        var (firstHost, _) = SplitEndpoint(hosts[0], port);
        Host = firstHost;
        Port = port;
        _database = b.Database;
        _tlsClientCert = b.TlsClientCert;
        _tlsClientKey = b.TlsClientKey;
        AuthMechanism = b.AuthMechanism;
        // A server with client_tls = required refuses plaintext outright, so
        // without TLS such a cluster is unreachable. Any knob turns it on.
        Tls = b.Tls || b.TlsCa.Length > 0 || b.TlsInsecure || _tlsClientCert.Length > 0
              || AuthMechanism == SkaidbAuthMechanism.Certificate;
        TlsCa = b.TlsCa;
        TlsInsecure = b.TlsInsecure;
        TlsServerName = b.TlsServerName;
        string seeds = b.Seeds;
        if (seeds.Length > 0)
        {
            Seeds = seeds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
        else
        {
            // Every Host entry is a seed; one without a port gets Port.
            var list = new List<string>(hosts.Length);
            foreach (var h in hosts)
            {
                var (hh, pp) = SplitEndpoint(h, port);
                list.Add(hh.Contains(':') ? $"[{hh}]:{pp}" : $"{hh}:{pp}");
            }
            Seeds = list;
        }
        User = b.Username;
        _userExplicit = b.ContainsKey("Username");
        _password = b.Password;
        Consistency = b.Consistency;
        Timeout = b.Timeout ?? TimeSpan.FromSeconds(10);
        _connectionString = b.ConnectionString;
    }

    /// <summary>
    /// Split "host", "host:port" or "[v6]:port" into its parts; a bare IPv6
    /// address (several colons, no brackets) is all host.
    /// </summary>
    internal static (string Host, int Port) SplitEndpoint(string endpoint, int defaultPort)
    {
        string ep = endpoint.Trim();
        if (ep.StartsWith('['))
        {
            int close = ep.IndexOf(']');
            if (close < 0) throw new SkaidbException($"malformed endpoint '{endpoint}'");
            string h6 = ep.Substring(1, close - 1);
            string rest = ep.Substring(close + 1);
            if (rest.Length == 0) return (h6, defaultPort);
            if (!rest.StartsWith(':')) throw new SkaidbException($"malformed endpoint '{endpoint}'");
            return (h6, ParsePort(rest.Substring(1), endpoint));
        }
        int first = ep.IndexOf(':');
        if (first < 0 || first != ep.LastIndexOf(':')) return (ep, defaultPort);
        return (ep.Substring(0, first), ParsePort(ep.Substring(first + 1), endpoint));
    }

    private static int ParsePort(string s, string endpoint)
    {
        if (!int.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out int p) || p < 1 || p > 65535)
            throw new SkaidbException($"invalid port in endpoint '{endpoint}'");
        return p;
    }

    /// <summary>
    /// The connection string. Setting it re-reads every setting and is only
    /// allowed while the connection is closed. Reading it back gives the
    /// normalised form (canonical key names), password included.
    /// </summary>
    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_open) throw new InvalidOperationException("the connection string cannot change while the connection is open");
            Configure(value ?? "");
        }
    }

    /// <summary>
    /// Endpoints to try, in shuffled order, until one connects. skaidb is
    /// leaderless, so any node serves — there is no primary to discover.
    /// From the <c>Seeds</c> key, else every entry of <c>Host</c>.
    /// </summary>
    public IReadOnlyList<string> Seeds { get; private set; } = Array.Empty<string>();

    private string _database = "";
    /// <summary>Session database, selected with USE right after connecting (and after every re-dial).</summary>
    public override string Database => _database;
    /// <summary>The seed list, comma-separated.</summary>
    public override string DataSource => string.Join(",", Seeds);
    /// <summary>
    /// The wire protocol does not report a server version, so this is empty;
    /// <c>SELECT version()</c> returns it.
    /// </summary>
    public override string ServerVersion => "";
    /// <summary>Open, Closed, or Open|Fetching while a <see cref="Stream"/> holds the connection.</summary>
    public override ConnectionState State =>
        !_open ? ConnectionState.Closed
        : _streaming ? ConnectionState.Open | ConnectionState.Fetching
        : ConnectionState.Open;
    /// <summary><see cref="Timeout"/> in whole seconds.</summary>
    public override int ConnectionTimeout => (int)Timeout.TotalSeconds;

    /// <summary>Whether to wrap the connection in TLS.</summary>
    public bool Tls { get; private set; }
    /// <summary>PEM CA bundle used to verify the server certificate.</summary>
    public string TlsCa { get; private set; } = "";
    /// <summary>Encrypt without verifying the certificate. Development only.</summary>
    public bool TlsInsecure { get; private set; }
    /// <summary>SNI name; must match a SAN on the server certificate.</summary>
    public string TlsServerName { get; private set; } = "skaidb";
    private string _tlsClientCert = "";
    private string _tlsClientKey = "";
    /// <summary>How the connection authenticates: SCRAM (default) or the TLS client certificate.</summary>
    public SkaidbAuthMechanism AuthMechanism { get; private set; }

    internal static SkaidbConsistency ParseConsistency(string value)
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

    /// <summary>
    /// Dial a seed and authenticate (SCRAM-SHA-256, or the TLS client
    /// certificate with <c>AuthMechanism=Certificate</c>). Idempotent; works
    /// again after <see cref="Close"/>.
    /// </summary>
    public override void Open()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (_open) return;
        OpenCore();
        OnStateChange(new StateChangeEventArgs(ConnectionState.Closed, ConnectionState.Open));
    }

    private void OpenCore()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(SkaidbConnection));
        if (_open) return;
        _broken = false;   // whatever broke the old socket died with it
        if (AuthMechanism == SkaidbAuthMechanism.Certificate && _tlsClientCert.Length == 0)
            throw new SkaidbException(
                "certificate authentication needs TLS with a client certificate (TlsClientCert, TlsClientKey)");

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
                var (h, p) = SplitEndpoint(ep, Port);
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
                    last = e is AggregateException agg && agg.InnerExceptions.Count == 1
                        ? agg.InnerExceptions[0] : e;
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
            if (AuthMechanism == SkaidbAuthMechanism.Certificate)
                HandshakeCertificate(_userExplicit ? User : "");
            else
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
            Exception root = e is AggregateException agg && agg.InnerExceptions.Count == 1
                ? agg.InnerExceptions[0] : e;
            throw new SkaidbException($"connect failed: {root.Message}", root);
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
        var ssl = new SslStream(raw, leaveInnerStreamOpen: false);
        var options = new SslClientAuthenticationOptions
        {
            TargetHost = TlsServerName,
            RemoteCertificateValidationCallback = cb,
        };
        if (_tlsClientCert.Length > 0)
        {
            var client = LoadClientCertificate(_tlsClientCert, _tlsClientKey);
            options.ClientCertificates = new X509CertificateCollection { client };
            // Always offer it: the default selection drops a certificate
            // whose issuer is not in the server's acceptable-CA list, and
            // then the server sees no certificate at all.
            options.LocalCertificateSelectionCallback = (_, _, _, _, _) => client;
        }
        ssl.AuthenticateAsClient(options);
        return ssl;
    }

    /// <summary>
    /// Load the PEM client certificate and its private key (a separate key
    /// file, or both in <paramref name="certPath"/> when the key path is empty).
    /// </summary>
    internal static X509Certificate2 LoadClientCertificate(string certPath, string keyPath)
    {
        var cert = keyPath.Length > 0
            ? X509Certificate2.CreateFromPemFile(certPath, keyPath)
            : X509Certificate2.CreateFromPemFile(certPath);
        if (OperatingSystem.IsWindows())
        {
            // SChannel will not use an ephemeral (never persisted) key for
            // client authentication; a PKCS#12 round-trip gives it one.
            var pfx = cert.Export(X509ContentType.Pkcs12);
            cert.Dispose();
#pragma warning disable SYSLIB0057 // X509CertificateLoader is .NET 9+
            cert = new X509Certificate2(pfx);
#pragma warning restore SYSLIB0057
        }
        return cert;
    }

    private void CleanupSocket()
    {
        try { _stream?.Dispose(); } catch { /* ignore */ }
        try { _tcp?.Dispose(); } catch { /* ignore */ }
        _stream = null;
        _tcp = null;
    }

    /// <summary>Create a command bound to this connection.</summary>
    public new SkaidbCommand CreateCommand() => new SkaidbCommand(this);

    protected override DbCommand CreateDbCommand() => CreateCommand();

    /// <summary>Create a <see cref="SkaidbBatch"/> bound to this connection.</summary>
    public new SkaidbBatch CreateBatch() => new SkaidbBatch(this);

    public override bool CanCreateBatch => true;

    protected override DbBatch CreateDbBatch() => CreateBatch();

    protected override DbProviderFactory DbProviderFactory => SkaidbFactory.Instance;

    /// <summary>
    /// Not supported: the wire protocol has no transactions. Every statement
    /// autocommits (and a batch commits row by row).
    /// </summary>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        throw new NotSupportedException(
            "skaidb connections have no transactions: every statement autocommits");

    /// <summary>
    /// Switch the session database: runs USE now when open, and on every
    /// later (re-)dial.
    /// </summary>
    public override void ChangeDatabase(string databaseName)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new ArgumentException("database name must not be empty", nameof(databaseName));
        if (_open)
        {
            using var use = CreateCommand();
            use.CommandText = "USE \"" + databaseName.Replace("\"", "\"\"") + "\"";
            use.ExecuteNonQuery();
        }
        _database = databaseName;
    }

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
    public bool IsUsable => !_disposed && _open && !_broken && !_streaming && !SocketDead();

    /// <summary>
    /// True when the peer has closed the socket while it sat idle: readable
    /// with nothing to read means EOF. A zero-timeout poll, so cheap enough
    /// for a pool to ask on every Acquire/Release. Only meaningful between
    /// requests; while one is in flight the flags above answer first.
    /// </summary>
    private bool SocketDead()
    {
        var tcp = _tcp;
        if (tcp is null) return true;
        try
        {
            var s = tcp.Client;
            if (!s.Connected) return true;
            return s.Poll(0, SelectMode.SelectRead) && s.Available == 0;
        }
        catch (Exception)
        {
            return true;
        }
    }

    /// <summary>
    /// Release the socket. The connection keeps its settings and
    /// <see cref="Open"/> dials again; prepared statements die with the socket.
    /// </summary>
    public override void Close()
    {
        bool wasOpen = _open;
        _open = false;
        _broken = false;
        _prepared.Clear();
        CleanupSocket();
        if (wasOpen) OnStateChange(new StateChangeEventArgs(ConnectionState.Open, ConnectionState.Closed));
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            Close();
            _disposed = true;
        }
        base.Dispose(disposing);
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
        // A peer that closed the socket while it sat idle (a node restart, an
        // idle timeout) is safe to re-dial now: nothing has been sent yet, so
        // there is no ambiguous write to worry about.
        if (!_broken && _open && SocketDead()) _broken = true;
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
            OpenCore();
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
        // One write for header + payload: with NoDelay set, two writes would
        // be two segments on the wire for every request.
        var frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)payload.Length); // length is BIG-endian
        Array.Copy(payload, 0, frame, 4, payload.Length);
        try
        {
            _stream.Write(frame, 0, frame.Length);
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
            var name = Encoding.UTF8.GetBytes(DriverName);
            var ver = Encoding.UTF8.GetBytes(DriverVersion);
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

        // AuthStart: u8 10, str username, str client_nonce (no mechanism
        // byte: its absence means SCRAM-SHA-256 to every server version)
        WriteFrame(BuildAuthStart(user, clientNonce, null));

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

    /// <summary>
    /// AuthStart (PROTOCOL.md 2, 2.4): u8 10, str username, str client_nonce,
    /// then the optional mechanism byte.
    /// </summary>
    internal static byte[] BuildAuthStart(string user, string clientNonce, SkaidbAuthMechanism? mechanism)
    {
        var w = new BinWriter();
        w.U8(10);
        w.Str(user);
        w.Str(clientNonce);
        if (mechanism.HasValue) w.U8((byte)mechanism.Value);
        return w.ToArray();
    }

    /// <summary>
    /// EXTERNAL (PROTOCOL.md 2.4): the TLS client certificate is the
    /// credential and its Common Name the username (<paramref name="user"/>
    /// may be empty, or must equal it). No exchange follows AuthStart; the
    /// server answers AuthOutcome at once, and the 32 zero bytes it sends as
    /// the signature are NOT verified: TLS already authenticated the server.
    /// </summary>
    private void HandshakeCertificate(string user)
    {
        if (!Tls) throw new SkaidbException("certificate authentication needs TLS with a client certificate");
        WriteFrame(BuildAuthStart(user, "", SkaidbAuthMechanism.Certificate));
        CheckCertificateOutcome(ReadFrame());
    }

    /// <summary>Parse the AuthOutcome that answers an EXTERNAL AuthStart.</summary>
    internal static void CheckCertificateOutcome(byte[] payload)
    {
        var r = new BinReader(payload);
        if (r.U8() != 13) throw new SkaidbException("bad handshake outcome");
        if (r.U8() != 1) throw new SkaidbException($"authentication denied: {r.Text()}");
        r.Take(32);   // the placeholder signature: present, deliberately unchecked
    }

    /// <summary>PBKDF2-HMAC-SHA-256(password, salt, iterations, dkLen = 32).</summary>
    internal static byte[] SaltedPassword(string password, byte[] salt, int iterations) =>
        Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(password), salt, iterations, HashAlgorithmName.SHA256, 32);

    internal static (byte[] proof, byte[] serverSig) ScramProof(
        string password, byte[] salt, int iterations, byte[] authMessage)
    {
        byte[] salted = SaltedPassword(password, salt, iterations);

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

        // Nothing from the previous stream may leak into this one: a
        // non-row reply sets no columns.
        StreamColumns = Array.Empty<string>();
        StreamAffected = null;

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
            if (tag == 1) { StreamAffected = (long)first.U64(); yield break; }   // Mutation
            if (tag == 2) yield break;                                           // Ddl
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

    /// <summary>
    /// Column names of the most recent <see cref="Stream"/> call; empty when
    /// that statement produced no rows (a mutation or DDL) or failed before
    /// its header.
    /// </summary>
    public IReadOnlyList<string> StreamColumns { get; private set; } = Array.Empty<string>();

    /// <summary>
    /// Affected-row count of the most recent <see cref="Stream"/> call when
    /// the statement was a mutation (the server answers a streamed
    /// INSERT/UPDATE/DELETE with a count, not rows); null otherwise.
    /// </summary>
    public long? StreamAffected { get; private set; }

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
        if (tag == 3)
        {
            string msg = r.Text();
            // Only a statement KIND the server refuses (DDL, USE, SHOW …) or
            // a server too old for OP_PREPARE falls back to client-side
            // binding. Any other prepare error — a parse error, an unknown
            // column — is the statement's real error and surfaces as such;
            // retrying it as text would fail again, or fail on a value the
            // text binder cannot render and hide the real message.
            if (msg.Contains("cannot be prepared") || msg.Contains("unknown opcode"))
                throw new UnpreparableException(msg);
            throw new SkaidbException(msg);
        }
        throw new SkaidbException($"unexpected prepare response tag {tag}");
    }

    /// <summary>
    /// Run one statement with `?` placeholders: with values, through a
    /// server-side prepared statement so they travel TYPED (arrays and
    /// documents have no SQL literal form); a statement kind the server will
    /// not prepare falls back to client-side text binding.
    /// </summary>
    internal QueryResult RunStatement(string sql, IReadOnlyList<object?> values, SkaidbConsistency consistency)
    {
        if (values.Count > 0)
        {
            try
            {
                var (id, n) = PrepareServer(sql);
                if (n != values.Count)
                    throw new SkaidbException($"statement expects {n} parameters, got {values.Count}");
                return ExecutePrepared(id, values, consistency);
            }
            catch (UnpreparableException)
            {
                // Statement kind the server will not prepare: bind as text.
            }
        }
        return Query(ParameterBinder.Bind(sql, values), consistency);
    }

    /// <summary>Prepare <paramref name="sql"/> and run it once per row in one OP_EXECUTE_BATCH; the total affected.</summary>
    internal long RunBatch(string sql, IReadOnlyList<IReadOnlyList<object?>> rows, SkaidbConsistency consistency)
    {
        var (id, n) = PrepareServer(sql);
        foreach (var r in rows)
        {
            if (r.Count != n)
                throw new SkaidbException($"batch row expects {n} parameters, got {r.Count}");
        }
        var result = ExecuteBatch(id, rows, consistency);
        return result.Kind == QueryResultKind.Mutation ? (long)result.Affected : 0;
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
            case ulong ul:
                // Int is i64; the upper half of ulong's range travels as an
                // exact Decimal (scale 0) rather than being truncated.
                if (ul <= long.MaxValue) { w.U8(2); w.I64((long)ul); }
                else EncodeDecimal(new BigInteger(ul), 0, w);
                return;
            case BigInteger bi:
                if (bi >= long.MinValue && bi <= long.MaxValue) { w.U8(2); w.I64((long)bi); }
                else EncodeDecimal(bi, 0, w);
                return;
            case float or double:
            {
                double d = Convert.ToDouble(v, CultureInfo.InvariantCulture);
                if (double.IsNaN(d) || double.IsInfinity(d))
                    throw new SkaidbException("cannot bind NaN/Infinity");
                w.U8(3); w.I64(BitConverter.DoubleToInt64Bits(d));
                return;
            }
            case decimal m:
            {
                // System.Decimal is a 96-bit magnitude, a sign and a scale of
                // 0..28: exactly a (mantissa, scale) pair, so no rounding.
                int[] bits = decimal.GetBits(m);
                uint scale = (uint)((bits[3] >> 16) & 0x7F);
                bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
                BigInteger mag = ((BigInteger)(uint)bits[2] << 64)
                               | ((BigInteger)(uint)bits[1] << 32)
                               | (uint)bits[0];
                EncodeDecimal(negative ? -mag : mag, scale, w);
                return;
            }
            case string str:
            {
                byte[] b = Encoding.UTF8.GetBytes(str);
                w.U8(5); w.U32((uint)b.Length); w.Raw(b);
                return;
            }
            case char ch:
            {
                byte[] b = Encoding.UTF8.GetBytes(ch.ToString());
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
            {
                // An Unspecified kind is taken as UTC, matching the client-side
                // binder; Local is converted.
                var utc = dt.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                    : dt.ToUniversalTime();
                w.U8(8); w.I64(new DateTimeOffset(utc).ToUnixTimeMilliseconds());
                return;
            }
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

    /// <summary>Decimal (tag 4): i128 little-endian mantissa, then u32 scale.</summary>
    private static void EncodeDecimal(BigInteger mantissa, uint scale, BinWriter w)
    {
        byte[] le = mantissa.ToByteArray(isUnsigned: false, isBigEndian: false);
        if (le.Length > 16)
            throw new SkaidbException("cannot bind decimal: mantissa does not fit 128 bits");
        var wide = new byte[16];
        Array.Copy(le, wide, le.Length);
        if (mantissa.Sign < 0)
            for (int i = le.Length; i < 16; i++) wide[i] = 0xFF;   // sign-extend
        w.U8(4); w.Raw(wide); w.U32(scale);
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
                // Keep the wire order (a Dictionary enumerates in insertion order
                // as long as nothing is removed). The server stores documents with
                // their keys sorted, so that is the order a stored document has here.
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

// ---- ADO.NET: commands ------------------------------------------------------

/// <summary>
/// A SQL command: an ADO.NET <see cref="DbCommand"/>. Set
/// <see cref="CommandText"/> and add values to <see cref="Parameters"/>, then
/// execute. Placeholders are <c>?</c> (bound in order), <c>@name</c> (bound by
/// <see cref="DbParameter.ParameterName"/>) or <c>$1</c>, <c>$2</c>, ... (bound
/// by position); a statement with parameters runs as a server-side prepared
/// statement with typed values.
/// </summary>
public sealed class SkaidbCommand : DbCommand
{
    private SkaidbConnection? _connection;
    private SkaidbConsistency? _consistency;
    private string _commandText = "";
    private CommandType _commandType = CommandType.Text;

    /// <summary>A command bound to <paramref name="connection"/>, at its consistency.</summary>
    public SkaidbCommand(SkaidbConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _consistency = connection.Consistency;
    }

    /// <summary>An unbound command; set <see cref="Connection"/> before executing.</summary>
    public SkaidbCommand() { }

    /// <summary>A command with its text, optionally bound to a connection.</summary>
    public SkaidbCommand(string commandText, SkaidbConnection? connection = null)
    {
        _commandText = commandText ?? "";
        _connection = connection;
        _consistency = connection?.Consistency;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? "";
    }

    /// <summary>
    /// Kept for ADO.NET callers; the connection's <see cref="SkaidbConnection.Timeout"/>
    /// is what bounds a request.
    /// </summary>
    public override int CommandTimeout { get; set; } = 30;

    /// <summary>
    /// <see cref="System.Data.CommandType.Text"/>, or
    /// <see cref="System.Data.CommandType.StoredProcedure"/>: <see cref="CommandText"/>
    /// is then a procedure name, run as <c>CALL name(?, ...)</c> with one
    /// placeholder per parameter.
    /// </summary>
    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            if (value != CommandType.Text && value != CommandType.StoredProcedure)
                throw new NotSupportedException($"CommandType.{value} is not supported");
            _commandType = value;
        }
    }

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;

    /// <summary>The connection the command runs on.</summary>
    public new SkaidbConnection? Connection
    {
        get => _connection;
        set => _connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = value switch
        {
            null => null,
            SkaidbConnection c => c,
            _ => throw new ArgumentException("a SkaidbCommand runs on a SkaidbConnection"),
        };
    }

    /// <summary>Parameters, bound to the statement's placeholders.</summary>
    public new SkaidbParameterCollection Parameters { get; } = new();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    /// <summary>Always null: skaidb has no transactions (setting one throws).</summary>
    protected override DbTransaction? DbTransaction
    {
        get => null;
        set
        {
            if (value is not null)
                throw new NotSupportedException("skaidb connections have no transactions");
        }
    }

    /// <summary>Per-command consistency override. Defaults to the connection's.</summary>
    public SkaidbConsistency Consistency
    {
        get => _consistency ?? _connection?.Consistency ?? SkaidbConsistency.Quorum;
        set => _consistency = value;
    }

    /// <summary>A no-op: a request cannot be cancelled once sent.</summary>
    public override void Cancel() { }

    public new SkaidbParameter CreateParameter() => new SkaidbParameter();

    protected override DbParameter CreateDbParameter() => CreateParameter();

    /// <summary>
    /// Prepare the statement on the server now (it is otherwise prepared on
    /// first execution). A statement kind the server does not prepare is left
    /// to run as text.
    /// </summary>
    public override void Prepare()
    {
        var conn = RequireOpen();
        var (sql, _) = Bound();
        try { conn.PrepareServer(sql); }
        catch (UnpreparableException) { /* runs as text */ }
    }

    /// <summary>Execute and return a reader over the result rows. One reader per execution.</summary>
    public new SkaidbDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

    /// <summary>
    /// Execute and return a reader. The result is fully received, so the
    /// connection is free again as soon as this returns. With
    /// <see cref="CommandBehavior.CloseConnection"/> closing the reader
    /// closes the connection.
    /// </summary>
    public new SkaidbDataReader ExecuteReader(CommandBehavior behavior)
    {
        var result = Run();
        var close = (behavior & CommandBehavior.CloseConnection) != 0 ? _connection : null;
        return SkaidbDataReader.FromResult(result, close);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteReader(behavior);

    /// <summary>Execute a statement and return the number of affected rows (0 for DDL/SELECT).</summary>
    public override int ExecuteNonQuery()
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
    public override object? ExecuteScalar()
    {
        var result = Run();
        if (result.Kind == QueryResultKind.Rows && result.Rows is { Count: > 0 } rows)
        {
            var first = rows[0];
            return first.Length > 0 ? first[0] : null;
        }
        return null;
    }

    private SkaidbConnection RequireOpen()
    {
        var conn = _connection ?? throw new InvalidOperationException("the command has no Connection");
        if (!conn.IsOpen) throw new SkaidbException("connection is not open");
        return conn;
    }

    /// <summary>The statement as the server sees it (`?` placeholders) and the values in order.</summary>
    internal (string Sql, IReadOnlyList<object?> Values) Bound()
    {
        string text = _commandText;
        if (_commandType == CommandType.StoredProcedure)
        {
            var qs = new string[Parameters.Count];
            Array.Fill(qs, "?");
            return ($"CALL {text}({string.Join(", ", qs)})", Parameters.Values());
        }
        return Placeholders.Rewrite(text, Parameters);
    }

    internal QueryResult Run()
    {
        var conn = RequireOpen();
        // Recover a transport that died since the last statement, before
        // anything is prepared on it.
        conn.EnsureLive();
        var (sql, values) = Bound();
        return conn.RunStatement(sql, values, Consistency);
    }

    /// <summary>
    /// Execute this statement once per row in ONE round-trip. Rows autocommit
    /// individually: a failure names the row and earlier rows stay applied,
    /// so the statement must be idempotent. Returns total affected rows.
    /// </summary>
    public long ExecuteBatch(IReadOnlyList<IReadOnlyList<object?>> rows)
    {
        if (rows.Count == 0) return 0;
        var conn = RequireOpen();
        var (sql, _) = Bound();
        return conn.RunBatch(sql, rows, Consistency);
    }
}

// ---- ADO.NET: parameters ----------------------------------------------------

/// <summary>
/// One statement parameter. Only input parameters exist. <see cref="DbType"/>
/// is informational: the value's own .NET type decides its wire type (see
/// docs/types.md).
/// </summary>
public sealed class SkaidbParameter : DbParameter
{
    private string _name = "";
    private DbType? _dbType;
    private ParameterDirection _direction = ParameterDirection.Input;

    public SkaidbParameter() { }

    public SkaidbParameter(string? parameterName, object? value)
    {
        _name = parameterName ?? "";
        Value = value;
    }

    /// <summary>
    /// Name for <c>@name</c> placeholders; a leading <c>@</c>, <c>:</c> or
    /// <c>$</c> is ignored when matching. Empty for a positional parameter.
    /// </summary>
    [AllowNull]
    public override string ParameterName
    {
        get => _name;
        set => _name = value ?? "";
    }

    public override object? Value { get; set; }

    /// <summary>The set DbType, else the one inferred from <see cref="Value"/>.</summary>
    public override DbType DbType
    {
        get => _dbType ?? InferDbType(Value);
        set => _dbType = value;
    }

    public override void ResetDbType() => _dbType = null;

    /// <summary>Only <see cref="ParameterDirection.Input"/> is supported.</summary>
    public override ParameterDirection Direction
    {
        get => _direction;
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("skaidb parameters are input-only");
            _direction = value;
        }
    }

    public override bool IsNullable { get; set; } = true;

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = "";

    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>The name without its placeholder prefix.</summary>
    internal string Key => _name.TrimStart('@', ':', '$');

    private static DbType InferDbType(object? v) => v switch
    {
        null or DBNull => DbType.Object,
        bool => DbType.Boolean,
        byte => DbType.Byte,
        sbyte => DbType.SByte,
        short => DbType.Int16,
        ushort => DbType.UInt16,
        int => DbType.Int32,
        uint => DbType.UInt32,
        long => DbType.Int64,
        ulong => DbType.UInt64,
        float => DbType.Single,
        double => DbType.Double,
        decimal => DbType.Decimal,
        string or char => DbType.String,
        byte[] => DbType.Binary,
        Guid => DbType.Guid,
        DateTimeOffset => DbType.DateTimeOffset,
        DateTime => DbType.DateTime,
        _ => DbType.Object,
    };
}

/// <summary>
/// The parameters of a <see cref="SkaidbCommand"/>. <see cref="Add(object?)"/>
/// takes either a <see cref="SkaidbParameter"/> or a bare value, which becomes
/// a positional parameter.
/// </summary>
public sealed class SkaidbParameterCollection : DbParameterCollection
{
    private readonly List<SkaidbParameter> _items = new();

    public override int Count => _items.Count;

    public override object SyncRoot => ((ICollection)_items).SyncRoot;

    /// <summary>
    /// Add a parameter: a <see cref="DbParameter"/> is added as itself (a
    /// foreign one is copied), anything else is a positional value.
    /// </summary>
    public override int Add(object? value)
    {
        _items.Add(Wrap(value));
        return _items.Count - 1;
    }

    /// <summary>Add a parameter (a null adds a positional NULL value).</summary>
    public SkaidbParameter Add(SkaidbParameter? parameter)
    {
        var p = parameter ?? new SkaidbParameter(null, null);
        _items.Add(p);
        return p;
    }

    /// <summary>Add a named parameter, for an <c>@name</c> placeholder.</summary>
    public SkaidbParameter AddWithValue(string parameterName, object? value) =>
        Add(new SkaidbParameter(parameterName, value));

    public override void AddRange(Array values)
    {
        foreach (var v in values) Add(v);
    }

    public override void Clear() => _items.Clear();

    public override bool Contains(object? value) => IndexOf(value) >= 0;

    public override bool Contains(string value) => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index) => ((ICollection)_items).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _items.GetEnumerator();

    public override int IndexOf(object? value) =>
        value is SkaidbParameter p ? _items.IndexOf(p) : -1;

    public override int IndexOf(string parameterName)
    {
        string key = parameterName.TrimStart('@', ':', '$');
        for (int i = 0; i < _items.Count; i++)
            if (string.Equals(_items[i].Key, key, StringComparison.Ordinal)) return i;
        for (int i = 0; i < _items.Count; i++)
            if (string.Equals(_items[i].Key, key, StringComparison.OrdinalIgnoreCase)) return i;
        return -1;
    }

    public override void Insert(int index, object? value) => _items.Insert(index, Wrap(value));

    public override void Remove(object? value)
    {
        if (value is SkaidbParameter p) _items.Remove(p);
    }

    public override void RemoveAt(int index) => _items.RemoveAt(index);

    public override void RemoveAt(string parameterName) => _items.RemoveAt(IndexOfOrThrow(parameterName));

    protected override DbParameter GetParameter(int index) => _items[index];

    protected override DbParameter GetParameter(string parameterName) => _items[IndexOfOrThrow(parameterName)];

    protected override void SetParameter(int index, DbParameter value) => _items[index] = Wrap(value);

    protected override void SetParameter(string parameterName, DbParameter value) =>
        _items[IndexOfOrThrow(parameterName)] = Wrap(value);

    public new SkaidbParameter this[int index]
    {
        get => _items[index];
        set => _items[index] = value;
    }

    public new SkaidbParameter this[string parameterName]
    {
        get => _items[IndexOfOrThrow(parameterName)];
        set => _items[IndexOfOrThrow(parameterName)] = value;
    }

    private int IndexOfOrThrow(string parameterName)
    {
        int i = IndexOf(parameterName);
        if (i < 0) throw new IndexOutOfRangeException($"no parameter named '{parameterName}'");
        return i;
    }

    private static SkaidbParameter Wrap(object? value) => value switch
    {
        SkaidbParameter p => p,
        DbParameter other => new SkaidbParameter(other.ParameterName, other.Value),
        _ => new SkaidbParameter(null, value),
    };

    internal SkaidbParameter At(int i) => _items[i];

    /// <summary>The values in collection order.</summary>
    internal IReadOnlyList<object?> Values()
    {
        var v = new object?[_items.Count];
        for (int i = 0; i < v.Length; i++) v[i] = _items[i].Value;
        return v;
    }
}

/// <summary>
/// Maps <c>@name</c> and <c>$N</c> placeholders onto the server's positional
/// <c>?</c>. Text inside '...' literals, "..." identifiers and comments is
/// left alone; a statement with only <c>?</c> placeholders passes unchanged.
/// </summary>
internal static class Placeholders
{
    public static (string Sql, IReadOnlyList<object?> Values) Rewrite(string sql, SkaidbParameterCollection ps)
    {
        if (ps.Count == 0) return (sql, Array.Empty<object?>());
        var sb = new StringBuilder(sql.Length);
        var values = new List<object?>();
        bool rewrote = false, sawQmark = false;
        int i = 0, n = sql.Length;
        while (i < n)
        {
            char ch = sql[i];
            if (ch == '\'' || ch == '"')
            {
                int end = i + 1;
                while (end < n)
                {
                    if (sql[end] == ch)
                    {
                        if (end + 1 < n && sql[end + 1] == ch) { end += 2; continue; }   // doubled = escaped
                        break;
                    }
                    end++;
                }
                end = Math.Min(end + 1, n);
                sb.Append(sql, i, end - i);
                i = end;
                continue;
            }
            if (ch == '-' && i + 1 < n && sql[i + 1] == '-')
            {
                int end = sql.IndexOf('\n', i);
                end = end < 0 ? n : end;
                sb.Append(sql, i, end - i);
                i = end;
                continue;
            }
            if (ch == '/' && i + 1 < n && sql[i + 1] == '*')
            {
                int end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = end < 0 ? n : end + 2;
                sb.Append(sql, i, end - i);
                i = end;
                continue;
            }
            if (ch == '?') { sawQmark = true; values.Add(null); sb.Append(ch); i++; continue; }
            if (ch == '@' && i + 1 < n && IsIdentStart(sql[i + 1]) && (i == 0 || !IsIdentPart(sql[i - 1])))
            {
                int j = i + 1;
                while (j < n && IsIdentPart(sql[j])) j++;
                string name = sql.Substring(i + 1, j - i - 1);
                int idx = ps.IndexOf(name);
                if (idx < 0) throw new SkaidbException($"no parameter named '@{name}'");
                values.Add(ps.At(idx).Value);
                sb.Append('?');
                rewrote = true;
                i = j;
                continue;
            }
            if (ch == '$' && i + 1 < n && char.IsAsciiDigit(sql[i + 1]) && (i == 0 || !IsIdentPart(sql[i - 1])))
            {
                int j = i + 1;
                while (j < n && char.IsAsciiDigit(sql[j])) j++;
                int pos = int.Parse(sql.AsSpan(i + 1, j - i - 1), NumberStyles.None, CultureInfo.InvariantCulture);
                if (pos < 1 || pos > ps.Count)
                    throw new SkaidbException($"placeholder ${pos} has no parameter ({ps.Count} given)");
                values.Add(ps.At(pos - 1).Value);
                sb.Append('?');
                rewrote = true;
                i = j;
                continue;
            }
            sb.Append(ch);
            i++;
        }
        if (!rewrote) return (sql, ps.Values());
        if (sawQmark)
            throw new SkaidbException("a statement cannot mix ? placeholders with @name or $N placeholders");
        return (sb.ToString(), values);
    }

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';

    private static bool IsIdentPart(char c) => char.IsLetterOrDigit(c) || c == '_';
}

// ---- ADO.NET: data reader ---------------------------------------------------

/// <summary>
/// Forward-only reader over a result: an ADO.NET <see cref="DbDataReader"/>.
/// Created by <see cref="SkaidbCommand.ExecuteReader()"/>. The whole result
/// is already received, so reading never touches the socket.
/// </summary>
public sealed class SkaidbDataReader : DbDataReader
{
    private string[] _columns;
    private List<object?[]> _rows;
    private int _pos = -1;
    private readonly List<QueryResult> _more;
    private readonly int _recordsAffected;
    private readonly SkaidbConnection? _closeOnClose;
    private bool _closed;

    internal SkaidbDataReader(string[] columns, List<object?[]> rows, List<QueryResult>? more = null,
                              int recordsAffected = -1, SkaidbConnection? closeOnClose = null)
    {
        _columns = columns;
        _rows = rows;
        _more = more ?? new List<QueryResult>();
        _recordsAffected = recordsAffected;
        _closeOnClose = closeOnClose;
    }

    internal static SkaidbDataReader FromResult(QueryResult result, SkaidbConnection? closeOnClose)
    {
        switch (result.Kind)
        {
            case QueryResultKind.Rows:
                return new SkaidbDataReader(result.Columns!, result.Rows!, result.MoreSets, -1, closeOnClose);
            case QueryResultKind.Mutation:
            {
                ulong n = result.Affected;
                return new SkaidbDataReader(Array.Empty<string>(), new List<object?[]>(), null,
                    n > int.MaxValue ? int.MaxValue : (int)n, closeOnClose);
            }
            default:
                // Non-row results expose an empty reader (FieldCount 0, Read() false).
                return new SkaidbDataReader(Array.Empty<string>(), new List<object?[]>(), null, -1, closeOnClose);
        }
    }

    /// <summary>Advance to the next result set of a multi-set reply (a CALL whose
    /// body EMITted, or a <see cref="SkaidbBatch"/>). Returns false when there is none.</summary>
    public override bool NextResult()
    {
        ThrowIfClosed();
        if (_more.Count == 0) return false;
        var next = _more[0];
        _more.RemoveAt(0);
        _columns = next.Columns ?? new string[0];
        _rows = next.Rows ?? new List<object?[]>();
        _pos = -1;
        return true;
    }

    /// <summary>Number of columns in the current result set (0 for a mutation or DDL).</summary>
    public override int FieldCount => _columns.Length;

    /// <summary>Rows changed by a mutation; -1 for a SELECT or DDL.</summary>
    public override int RecordsAffected => _recordsAffected;

    public override bool HasRows => _rows.Count > 0;

    public override bool IsClosed => _closed;

    public override int Depth => 0;

    /// <summary>Advance to the next row. Returns false when exhausted.</summary>
    public override bool Read()
    {
        ThrowIfClosed();
        if (_pos + 1 >= _rows.Count) { _pos = _rows.Count; return false; }
        _pos++;
        return true;
    }

    private void ThrowIfClosed()
    {
        if (_closed) throw new InvalidOperationException("the reader is closed");
    }

    private object?[] Current
    {
        get
        {
            ThrowIfClosed();
            if (_pos < 0) throw new SkaidbException("call Read() before accessing fields");
            if (_pos >= _rows.Count) throw new SkaidbException("no current row");
            return _rows[_pos];
        }
    }

    public override string GetName(int ordinal) => _columns[ordinal];

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.Ordinal))
                return i;
        for (int i = 0; i < _columns.Length; i++)
            if (string.Equals(_columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new SkaidbException($"no column named '{name}'");
    }

    public override bool IsDBNull(int ordinal) => Current[ordinal] is null;

    /// <summary>Raw value; null maps to <see cref="DBNull.Value"/>.</summary>
    public override object GetValue(int ordinal) => Current[ordinal] ?? DBNull.Value;

    public override int GetValues(object[] values)
    {
        var row = Current;
        int n = Math.Min(values.Length, row.Length);
        for (int i = 0; i < n; i++) values[i] = row[i] ?? DBNull.Value;
        return n;
    }

    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override byte GetByte(int ordinal) => Convert.ToByte(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override short GetInt16(int ordinal) => Convert.ToInt16(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override long GetInt64(int ordinal) => Convert.ToInt64(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override int GetInt32(int ordinal) => Convert.ToInt32(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override double GetDouble(int ordinal) => Convert.ToDouble(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override float GetFloat(int ordinal) => Convert.ToSingle(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(NonNull(ordinal), CultureInfo.InvariantCulture);

    public override string GetString(int ordinal)
    {
        object v = NonNull(ordinal);
        return v as string ?? Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
    }

    public override char GetChar(int ordinal)
    {
        string s = GetString(ordinal);
        if (s.Length == 0) throw new InvalidCastException($"column {ordinal} is an empty string");
        return s[0];
    }

    public override Guid GetGuid(int ordinal)
    {
        object v = NonNull(ordinal);
        return v is Guid g ? g : Guid.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!);
    }

    /// <summary>A timestamp as a UTC <see cref="DateTime"/>.</summary>
    public override DateTime GetDateTime(int ordinal) => NonNull(ordinal) switch
    {
        DateTimeOffset dto => dto.UtcDateTime,
        DateTime dt => dt,
        var v => Convert.ToDateTime(v, CultureInfo.InvariantCulture),
    };

    public DateTimeOffset GetDateTimeOffset(int i) => (DateTimeOffset)NonNull(i);

    public byte[] GetBytes(int i) => (byte[])NonNull(i);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] data = GetBytes(ordinal);
        if (buffer is null) return data.Length;
        int n = (int)Math.Max(0, Math.Min(length, data.Length - dataOffset));
        Array.Copy(data, dataOffset, buffer, bufferOffset, n);
        return n;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        string data = GetString(ordinal);
        if (buffer is null) return data.Length;
        int n = (int)Math.Max(0, Math.Min(length, data.Length - dataOffset));
        data.CopyTo((int)dataOffset, buffer, bufferOffset, n);
        return n;
    }

    /// <summary>
    /// The value as <typeparamref name="T"/>. Beyond an exact match it
    /// converts numbers, a timestamp to <see cref="DateTime"/> (UTC), a
    /// wide-decimal string to <see cref="decimal"/> when it fits, and text to
    /// <see cref="Guid"/>. NULL gives default for a reference or nullable
    /// <typeparamref name="T"/> and throws otherwise.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        object? v = Current[ordinal];
        if (v is null)
        {
            if (default(T) is null) return default!;
            if (typeof(T) == typeof(DBNull)) return (T)(object)DBNull.Value;
            throw new SkaidbException($"column {ordinal} is NULL");
        }
        if (v is T t) return t;
        var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        object converted = target switch
        {
            _ when target == typeof(DateTime) => GetDateTime(ordinal),
            _ when target == typeof(Guid) => GetGuid(ordinal),
            _ when target == typeof(string) => GetString(ordinal),
            _ when target == typeof(decimal) && v is string s =>
                decimal.Parse(s, NumberStyles.Number, CultureInfo.InvariantCulture),
            _ when v is IConvertible => Convert.ChangeType(v, target, CultureInfo.InvariantCulture),
            _ => throw new InvalidCastException($"column {ordinal} is {v.GetType().Name}, not {typeof(T).Name}"),
        };
        return (T)converted;
    }

    /// <summary>
    /// The CLR type of a column. The wire format carries no column types, so
    /// this is the type of the column's first non-NULL value in the current
    /// result set (<see cref="object"/> when there is none).
    /// </summary>
    public override Type GetFieldType(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _columns.Length) throw new IndexOutOfRangeException($"no column {ordinal}");
        foreach (var row in _rows)
            if (ordinal < row.Length && row[ordinal] is { } v) return v.GetType();
        return typeof(object);
    }

    /// <summary>The skaidb type name of <see cref="GetFieldType"/> (int, float, decimal, string, ...).</summary>
    public override string GetDataTypeName(int ordinal)
    {
        var t = GetFieldType(ordinal);
        if (t == typeof(long)) return "int";
        if (t == typeof(double)) return "float";
        if (t == typeof(decimal)) return "decimal";
        if (t == typeof(string)) return "string";
        if (t == typeof(bool)) return "bool";
        if (t == typeof(byte[])) return "bytes";
        if (t == typeof(Guid)) return "uuid";
        if (t == typeof(DateTimeOffset)) return "timestamp";
        if (t == typeof(object[])) return "array";
        if (t == typeof(Dictionary<string, object?>)) return "document";
        return "unknown";
    }

    /// <summary>
    /// A minimal schema table: ColumnName, ColumnOrdinal, ColumnSize (-1),
    /// DataType, DataTypeName and AllowDBNull (always true) per column.
    /// </summary>
    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
        table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
        table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
        table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
        table.Columns.Add("DataTypeName", typeof(string));
        table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
        for (int i = 0; i < _columns.Length; i++)
            table.Rows.Add(_columns[i], i, -1, GetFieldType(i), GetDataTypeName(i), true);
        return table;
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    private object NonNull(int i)
    {
        object? v = Current[i];
        if (v is null) throw new SkaidbException($"column {i} is NULL");
        return v;
    }

    /// <summary>Value by ordinal (DBNull for null).</summary>
    public override object this[int ordinal] => GetValue(ordinal);

    /// <summary>Value by column name (DBNull for null).</summary>
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <summary>Close the reader (and the connection, under <see cref="CommandBehavior.CloseConnection"/>).</summary>
    public override void Close()
    {
        if (_closed) return;
        _closed = true;
        _closeOnClose?.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Close();
        base.Dispose(disposing);
    }
}

// ---- ADO.NET: batches -------------------------------------------------------

/// <summary>
/// Several statements in one call: an ADO.NET <see cref="DbBatch"/>. Runs of
/// consecutive commands with the same parameterised statement go to the
/// server as ONE <c>OP_EXECUTE_BATCH</c> round-trip; everything else runs one
/// statement at a time. There are no transactions: each statement (each row
/// of a merged run) autocommits, and a failure leaves earlier ones applied.
/// </summary>
public sealed class SkaidbBatch : DbBatch
{
    private SkaidbConnection? _connection;

    public SkaidbBatch() { }

    public SkaidbBatch(SkaidbConnection connection) { _connection = connection; }

    public new SkaidbBatchCommandCollection BatchCommands { get; } = new();

    protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

    /// <summary>Kept for ADO.NET callers; the connection's timeout bounds each request.</summary>
    public override int Timeout { get; set; }

    public new SkaidbConnection? Connection
    {
        get => _connection;
        set => _connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = value switch
        {
            null => null,
            SkaidbConnection c => c,
            _ => throw new ArgumentException("a SkaidbBatch runs on a SkaidbConnection"),
        };
    }

    protected override DbTransaction? DbTransaction
    {
        get => null;
        set
        {
            if (value is not null) throw new NotSupportedException("skaidb connections have no transactions");
        }
    }

    public override void Cancel() { }

    protected override DbBatchCommand CreateDbBatchCommand() => new SkaidbBatchCommand();

    public override void Prepare() { }

    public override Task PrepareAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>Run every command; the sum of the rows they changed.</summary>
    public override int ExecuteNonQuery()
    {
        long total = 0;
        foreach (var r in RunAll()) if (r.Kind == QueryResultKind.Mutation) total += (long)r.Affected;
        return total > int.MaxValue ? int.MaxValue : (int)total;
    }

    /// <summary>Run every command; the first value of the first row-producing one.</summary>
    public override object? ExecuteScalar()
    {
        foreach (var r in RunAll())
            if (r.Kind == QueryResultKind.Rows)
                return r.Rows is { Count: > 0 } rows && rows[0].Length > 0 ? rows[0][0] : null;
        return null;
    }

    /// <summary>
    /// Run every command; a reader whose result sets are the row-producing
    /// commands' results in order (<see cref="DbDataReader.NextResult"/> walks
    /// them). <see cref="DbDataReader.RecordsAffected"/> is the total changed.
    /// </summary>
    public new SkaidbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
    {
        var sets = new List<QueryResult>();
        long total = 0;
        foreach (var r in RunAll())
        {
            if (r.Kind == QueryResultKind.Rows)
            {
                sets.Add(new QueryResult(QueryResultKind.Rows, r.Columns, r.Rows, 0));
                sets.AddRange(r.MoreSets);
            }
            else if (r.Kind == QueryResultKind.Mutation) total += (long)r.Affected;
        }
        var close = (behavior & CommandBehavior.CloseConnection) != 0 ? _connection : null;
        int affected = sets.Count > 0 && total == 0 ? -1 : (int)Math.Min(total, int.MaxValue);
        if (sets.Count == 0)
            return new SkaidbDataReader(Array.Empty<string>(), new List<object?[]>(), null, affected, close);
        var first = sets[0];
        sets.RemoveAt(0);
        return new SkaidbDataReader(first.Columns!, first.Rows!, sets, affected, close);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteReader(behavior);

    protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<DbDataReader>(ExecuteReader(behavior));
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteNonQuery());
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteScalar());
    }

    /// <summary>
    /// Execute the commands in order, merging runs of the same parameterised
    /// statement into one OP_EXECUTE_BATCH. Each command's RecordsAffected is
    /// set; a command merged into a run reports -1 (the server returns only
    /// the run's total).
    /// </summary>
    private List<QueryResult> RunAll()
    {
        var conn = _connection ?? throw new InvalidOperationException("the batch has no Connection");
        if (!conn.IsOpen) throw new SkaidbException("connection is not open");
        var results = new List<QueryResult>();
        var cmds = BatchCommands.Items;
        int i = 0;
        while (i < cmds.Count)
        {
            var (sql, values) = cmds[i].Bound();
            int j = i + 1;
            var rows = new List<IReadOnlyList<object?>> { values };
            if (values.Count > 0)
            {
                while (j < cmds.Count)
                {
                    var (sql2, values2) = cmds[j].Bound();
                    if (sql2 != sql || values2.Count != values.Count) break;
                    rows.Add(values2);
                    j++;
                }
            }
            if (j - i > 1)
            {
                try
                {
                    long n = conn.RunBatch(sql, rows, conn.Consistency);
                    for (int k = i; k < j; k++) cmds[k].SetRecordsAffected(-1);
                    results.Add(new QueryResult(QueryResultKind.Mutation, null, null, (ulong)n));
                    i = j;
                    continue;
                }
                catch (UnpreparableException)
                {
                    // Not a preparable statement kind: run the run one by one.
                }
            }
            conn.EnsureLive();
            var r = conn.RunStatement(sql, values, conn.Consistency);
            cmds[i].SetRecordsAffected(r.Kind == QueryResultKind.Mutation
                ? (int)Math.Min(r.Affected, int.MaxValue) : -1);
            results.Add(r);
            i++;
        }
        return results;
    }
}

/// <summary>One statement of a <see cref="SkaidbBatch"/>.</summary>
public sealed class SkaidbBatchCommand : DbBatchCommand
{
    private string _commandText = "";
    private CommandType _commandType = CommandType.Text;
    private int _recordsAffected = -1;

    public SkaidbBatchCommand() { }

    public SkaidbBatchCommand(string commandText) { _commandText = commandText; }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? "";
    }

    /// <summary>Text or StoredProcedure, as on <see cref="SkaidbCommand.CommandType"/>.</summary>
    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            if (value != CommandType.Text && value != CommandType.StoredProcedure)
                throw new NotSupportedException($"CommandType.{value} is not supported");
            _commandType = value;
        }
    }

    /// <summary>Rows this command changed; -1 for a SELECT, DDL, or a command merged into an OP_EXECUTE_BATCH run.</summary>
    public override int RecordsAffected => _recordsAffected;

    internal void SetRecordsAffected(int n) => _recordsAffected = n;

    public new SkaidbParameterCollection Parameters { get; } = new();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    public override bool CanCreateParameter => true;

    public override DbParameter CreateParameter() => new SkaidbParameter();

    internal (string Sql, IReadOnlyList<object?> Values)Bound()
    {
        if (_commandType == CommandType.StoredProcedure)
        {
            var qs = new string[Parameters.Count];
            Array.Fill(qs, "?");
            return ($"CALL {_commandText}({string.Join(", ", qs)})", Parameters.Values());
        }
        return Placeholders.Rewrite(_commandText, Parameters);
    }
}

/// <summary>The commands of a <see cref="SkaidbBatch"/>.</summary>
public sealed class SkaidbBatchCommandCollection : DbBatchCommandCollection
{
    internal readonly List<SkaidbBatchCommand> Items = new();

    public override int Count => Items.Count;

    public override bool IsReadOnly => false;

    public override void Add(DbBatchCommand item) => Items.Add(Cast(item));

    /// <summary>Add a statement and return it, for adding its parameters.</summary>
    public SkaidbBatchCommand Add(string commandText)
    {
        var c = new SkaidbBatchCommand(commandText);
        Items.Add(c);
        return c;
    }

    public override void Clear() => Items.Clear();

    public override bool Contains(DbBatchCommand item) => item is SkaidbBatchCommand c && Items.Contains(c);

    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        for (int i = 0; i < Items.Count; i++) array[arrayIndex + i] = Items[i];
    }

    public override IEnumerator<DbBatchCommand> GetEnumerator() => Items.GetEnumerator();

    public override int IndexOf(DbBatchCommand item) => item is SkaidbBatchCommand c ? Items.IndexOf(c) : -1;

    public override void Insert(int index, DbBatchCommand item) => Items.Insert(index, Cast(item));

    public override bool Remove(DbBatchCommand item) => item is SkaidbBatchCommand c && Items.Remove(c);

    public override void RemoveAt(int index) => Items.RemoveAt(index);

    protected override DbBatchCommand GetBatchCommand(int index) => Items[index];

    protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => Items[index] = Cast(batchCommand);

    public new SkaidbBatchCommand this[int index]
    {
        get => Items[index];
        set => Items[index] = value;
    }

    private static SkaidbBatchCommand Cast(DbBatchCommand item) =>
        item as SkaidbBatchCommand ?? throw new ArgumentException("a SkaidbBatch holds SkaidbBatchCommand items");
}

// ---- ADO.NET: provider factory + connection string builder --------------------

/// <summary>
/// The ADO.NET provider factory. Register it once at startup to resolve the
/// provider by name:
/// <code>
/// DbProviderFactories.RegisterFactory(SkaidbFactory.ProviderInvariantName, SkaidbFactory.Instance);
/// var factory = DbProviderFactories.GetFactory("Skaidb");
/// </code>
/// </summary>
public sealed class SkaidbFactory : DbProviderFactory
{
    /// <summary>The provider invariant name: <c>Skaidb</c>.</summary>
    public const string ProviderInvariantName = "Skaidb";

    /// <summary>The one instance (DbProviderFactories looks this field up by name).</summary>
    public static readonly SkaidbFactory Instance = new();

    private SkaidbFactory() { }

    /// <summary>Register <see cref="Instance"/> under <see cref="ProviderInvariantName"/>.</summary>
    public static void Register() => DbProviderFactories.RegisterFactory(ProviderInvariantName, Instance);

    public override DbConnection CreateConnection() => new SkaidbConnection();

    public override DbCommand CreateCommand() => new SkaidbCommand();

    public override DbParameter CreateParameter() => new SkaidbParameter();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new SkaidbConnectionStringBuilder();

    public override bool CanCreateBatch => true;

    public override DbBatch CreateBatch() => new SkaidbBatch();

    public override DbBatchCommand CreateBatchCommand() => new SkaidbBatchCommand();
}

/// <summary>
/// Builds and parses skaidb connection strings. Keys are case-insensitive and
/// normalised to the canonical names below; unknown keys are an error.
/// </summary>
/// <remarks>
/// <list type="table">
/// <item><term>Host</term><description>(Server, Data Source) one host or a comma list: <c>a,b:7001,[::1]:7002</c>. Every entry is a seed; one without a port uses Port.</description></item>
/// <item><term>Port</term><description>default port, 7000</description></item>
/// <item><term>Username</term><description>(User, User Id, UserId, Uid) default <c>anonymous</c></description></item>
/// <item><term>Password</term><description>(Pwd)</description></item>
/// <item><term>Database</term><description>(Db) session database, selected with USE on every dial</description></item>
/// <item><term>Consistency</term><description>One, Quorum (default) or All</description></item>
/// <item><term>Timeout</term><description>(Connect Timeout, ConnectTimeout) seconds, default 10</description></item>
/// <item><term>Seeds</term><description>explicit endpoint list; overrides the Host list</description></item>
/// <item><term>Tls</term><description>true/false; implied by any other Tls* key</description></item>
/// <item><term>TlsCa</term><description>(Tls CA) PEM CA bundle for the server certificate</description></item>
/// <item><term>TlsInsecure</term><description>(Tls Insecure) encrypt without verifying. Development only</description></item>
/// <item><term>TlsServerName</term><description>(Tls Server Name) SNI / verified name, default <c>skaidb</c></description></item>
/// <item><term>TlsClientCert</term><description>(Tls Client Cert) PEM client certificate to present</description></item>
/// <item><term>TlsClientKey</term><description>(Tls Client Key) its PEM private key</description></item>
/// <item><term>AuthMechanism</term><description>(Auth Mechanism) <c>Scram</c> (default) or <c>Certificate</c></description></item>
/// </list>
/// </remarks>
public sealed class SkaidbConnectionStringBuilder : DbConnectionStringBuilder
{
    private static readonly Dictionary<string, string> Canonical = BuildCanonical();

    private static Dictionary<string, string> BuildCanonical()
    {
        var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        void Map(string canon, params string[] aliases)
        {
            d[canon] = canon;
            foreach (var a in aliases) d[a] = canon;
        }
        Map("Host", "Server", "DataSource", "Data Source");
        Map("Port");
        Map("Username", "User", "UserId", "User Id", "Uid");
        Map("Password", "Pwd");
        Map("Database", "Db");
        Map("Consistency");
        Map("Timeout", "ConnectTimeout", "Connect Timeout");
        Map("Seeds");
        Map("Tls");
        Map("TlsCa", "Tls CA");
        Map("TlsInsecure", "Tls Insecure");
        Map("TlsServerName", "Tls Server Name");
        Map("TlsClientCert", "Tls Client Cert", "TlsClientCertificate");
        Map("TlsClientKey", "Tls Client Key");
        Map("AuthMechanism", "Auth Mechanism");
        return d;
    }

    public SkaidbConnectionStringBuilder() { }

    public SkaidbConnectionStringBuilder(string? connectionString)
    {
        ConnectionString = connectionString ?? "";
    }

    private static string Canon(string keyword)
    {
        if (keyword is null) throw new ArgumentNullException(nameof(keyword));
        if (Canonical.TryGetValue(keyword.Trim(), out var c)) return c;
        throw new ArgumentException($"unknown connection string key: '{keyword}'", nameof(keyword));
    }

    /// <summary>A value by any key or alias; setting validates it (an invalid value is an ArgumentException).</summary>
    [AllowNull]
    public override object this[string keyword]
    {
        get => base[Canon(keyword)];
        set
        {
            string k = Canon(keyword);
            if (value is null) { base.Remove(k); return; }
            string s = Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim() ?? "";
            Validate(k, s);
            base[k] = s;
        }
    }

    private static void Validate(string key, string value)
    {
        switch (key)
        {
            case "Port":
                if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int p) || p < 1 || p > 65535)
                    throw new ArgumentException($"invalid Port '{value}'");
                break;
            case "Consistency":
                try { SkaidbConnection.ParseConsistency(value); }
                catch (SkaidbException e) { throw new ArgumentException(e.Message); }
                break;
            case "Timeout":
                if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double t) || t <= 0 || double.IsInfinity(t))
                    throw new ArgumentException($"invalid Timeout '{value}' (seconds)");
                break;
            case "AuthMechanism":
                ParseMechanism(value);
                break;
        }
    }

    private static SkaidbAuthMechanism ParseMechanism(string value) => value.Trim().ToLowerInvariant() switch
    {
        "" or "scram" or "scram-sha-256" or "password" => SkaidbAuthMechanism.Scram,
        "certificate" or "external" or "x509" => SkaidbAuthMechanism.Certificate,
        _ => throw new ArgumentException($"unknown AuthMechanism '{value}' (use Scram or Certificate)"),
    };

    public override bool ContainsKey(string keyword) =>
        Canonical.TryGetValue(keyword.Trim(), out var c) && base.ContainsKey(c);

    public override bool Remove(string keyword) =>
        Canonical.TryGetValue(keyword.Trim(), out var c) && base.Remove(c);

    public override bool ShouldSerialize(string keyword) =>
        Canonical.TryGetValue(keyword.Trim(), out var c) && base.ShouldSerialize(c);

    public override bool TryGetValue(string keyword, [NotNullWhen(true)] out object? value)
    {
        if (Canonical.TryGetValue(keyword.Trim(), out var c)) return base.TryGetValue(c, out value);
        value = null;
        return false;
    }

    private string Get(string key, string fallback) =>
        base.TryGetValue(key, out var v) && v is not null ? Convert.ToString(v, CultureInfo.InvariantCulture) ?? fallback : fallback;

    private static bool Truthy(string v) =>
        v.Equals("true", StringComparison.OrdinalIgnoreCase) || v == "1"
        || v.Equals("yes", StringComparison.OrdinalIgnoreCase) || v.Equals("on", StringComparison.OrdinalIgnoreCase);

    /// <summary>One host, or a comma-separated list of <c>host[:port]</c> seeds.</summary>
    public string Host { get => Get("Host", "localhost"); set => this["Host"] = value; }

    public int Port
    {
        get => int.Parse(Get("Port", "7000"), CultureInfo.InvariantCulture);
        set => this["Port"] = value;
    }

    public string Username { get => Get("Username", "anonymous"); set => this["Username"] = value; }

    public string Password { get => Get("Password", ""); set => this["Password"] = value; }

    public string Database { get => Get("Database", ""); set => this["Database"] = value; }

    public SkaidbConsistency Consistency
    {
        get => SkaidbConnection.ParseConsistency(Get("Consistency", "Quorum"));
        set => this["Consistency"] = value.ToString();
    }

    /// <summary>Connect/read timeout; null when unset (the connection then uses 10 s).</summary>
    public TimeSpan? Timeout
    {
        get => base.TryGetValue("Timeout", out var v) && v is not null
            ? TimeSpan.FromSeconds(double.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture))
            : null;
        set => this["Timeout"] = value?.TotalSeconds;
    }

    public string Seeds { get => Get("Seeds", ""); set => this["Seeds"] = value; }

    public bool Tls { get => Truthy(Get("Tls", "false")); set => this["Tls"] = value; }

    public string TlsCa { get => Get("TlsCa", ""); set => this["TlsCa"] = value; }

    public bool TlsInsecure { get => Truthy(Get("TlsInsecure", "false")); set => this["TlsInsecure"] = value; }

    public string TlsServerName { get => Get("TlsServerName", "skaidb"); set => this["TlsServerName"] = value; }

    public string TlsClientCert { get => Get("TlsClientCert", ""); set => this["TlsClientCert"] = value; }

    public string TlsClientKey { get => Get("TlsClientKey", ""); set => this["TlsClientKey"] = value; }

    public SkaidbAuthMechanism AuthMechanism
    {
        get => ParseMechanism(Get("AuthMechanism", "Scram"));
        set => this["AuthMechanism"] = value.ToString();
    }
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
