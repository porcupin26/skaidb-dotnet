// Certificate login (wire mechanism EXTERNAL, PROTOCOL.md 2.4): the frame
// builder and outcome parser directly, then the whole handshake over a real
// TLS session against an in-process server that demands a client
// certificate. Every certificate is minted in the test; nothing touches disk
// outside a temp directory.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Xunit;

namespace Skaidb.Tests;

public sealed class CertificateAuthTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "skaidb-dotnet-cert-" + Guid.NewGuid().ToString("N"));
    private readonly X509Certificate2 _serverCert;
    private readonly string _caPem, _clientPem, _clientKey, _clientCombined;

    public CertificateAuthTests()
    {
        Directory.CreateDirectory(_dir);
        using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var caReq = new CertificateRequest("CN=test-ca", caKey, HashAlgorithmName.SHA256);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        _serverCert = Issue(ca, "CN=skaidb", "skaidb", serverAuth: true);
        using var client = Issue(ca, "CN=ada", null, serverAuth: false);

        _caPem = Write("ca.pem", ca.ExportCertificatePem());
        _clientPem = Write("client.pem", client.ExportCertificatePem());
        string key = client.GetECDsaPrivateKey()!.ExportPkcs8PrivateKeyPem();
        _clientKey = Write("client.key", key);
        _clientCombined = Write("client-combined.pem", client.ExportCertificatePem() + "\n" + key);
    }

    private static X509Certificate2 Issue(X509Certificate2 ca, string subject, string? dns, bool serverAuth)
    {
        var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var req = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
        req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection
        {
            new Oid(serverAuth ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"),
        }, false));
        if (dns is not null)
        {
            var san = new SubjectAlternativeNameBuilder();
            san.AddDnsName(dns);
            req.CertificateExtensions.Add(san.Build());
        }
        var serial = new byte[8];
        RandomNumberGenerator.Fill(serial);
        using var pub = req.Create(ca, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddHours(12), serial);
        var withKey = pub.CopyWithPrivateKey(key);
        // SslStream on some platforms wants a persisted key for a server cert.
        return new X509Certificate2(withKey.Export(X509ContentType.Pkcs12));
    }

    private string Write(string name, string pem)
    {
        string p = Path.Combine(_dir, name);
        File.WriteAllText(p, pem);
        return p;
    }

    public void Dispose()
    {
        _serverCert.Dispose();
        try { Directory.Delete(_dir, true); } catch { /* best effort */ }
    }

    // ---- frames -------------------------------------------------------------------

    [Fact]
    public void AuthStart_carries_the_mechanism_byte_only_for_EXTERNAL()
    {
        Assert.Equal(new byte[] { 10, 0, 0, 0, 0, 0, 0, 0, 0, 2 },
            SkaidbConnection.BuildAuthStart("", "", SkaidbAuthMechanism.Certificate));
        Assert.Equal(new byte[] { 10, 3, 0, 0, 0, (byte)'a', (byte)'d', (byte)'a', 0, 0, 0, 0, 2 },
            SkaidbConnection.BuildAuthStart("ada", "", SkaidbAuthMechanism.Certificate));
        // SCRAM sends no mechanism byte at all: every server version reads that as SCRAM.
        Assert.Equal(new byte[] { 10, 1, 0, 0, 0, (byte)'u', 1, 0, 0, 0, (byte)'n' },
            SkaidbConnection.BuildAuthStart("u", "n", null));
    }

    [Fact]
    public void The_EXTERNAL_outcome_signature_is_not_verified_and_a_denial_carries_its_reason()
    {
        var ok = new byte[34];
        ok[0] = 13; ok[1] = 1;
        SkaidbConnection.CheckCertificateOutcome(ok);                            // 32 zero bytes
        RandomNumberGenerator.Fill(ok.AsSpan(2));
        SkaidbConnection.CheckCertificateOutcome(ok);                            // anything: TLS authenticated the server
        var denied = new List<byte> { 13, 0 };
        byte[] reason = Encoding.UTF8.GetBytes("certificate CN 'ada' maps to no role");
        denied.AddRange(BitConverter.GetBytes(reason.Length));
        denied.AddRange(reason);
        var e = Assert.Throws<SkaidbException>(() => SkaidbConnection.CheckCertificateOutcome(denied.ToArray()));
        Assert.Equal("authentication denied: certificate CN 'ada' maps to no role", e.Message);
        Assert.Throws<SkaidbException>(() => SkaidbConnection.CheckCertificateOutcome(new byte[] { 11 }));
        Assert.Throws<SkaidbException>(() => SkaidbConnection.CheckCertificateOutcome(new byte[] { 13, 1, 0 }));   // truncated
    }

    [Fact]
    public void Certificate_mode_without_a_client_certificate_fails_before_dialing()
    {
        using var c = new SkaidbConnection("Host=127.0.0.1;Port=1;AuthMechanism=Certificate;TlsInsecure=true");
        Assert.True(c.Tls);
        var e = Assert.Throws<SkaidbException>(() => c.Open());
        Assert.Contains("TlsClientCert", e.Message);
    }

    // ---- over TLS -----------------------------------------------------------------

    /// <summary>
    /// A TLS server that requires a client certificate, records the
    /// certificate and the AuthStart it got, answers AuthOutcome (ok or
    /// denied), then answers everything else (Hello, a query) with Ddl.
    /// </summary>
    private sealed class TlsServer : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly Thread _thread;
        public byte[]? AuthStart;
        public string? ClientSubject;
        public string? Error;
        public int Port { get; }

        public TlsServer(X509Certificate2 cert, string? deny)
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _thread = new Thread(() =>
            {
                try
                {
                    using var sock = _listener.AcceptTcpClient();
                    using var ssl = new SslStream(sock.GetStream());
                    ssl.AuthenticateAsServer(new SslServerAuthenticationOptions
                    {
                        ServerCertificate = cert,
                        ClientCertificateRequired = true,
                        RemoteCertificateValidationCallback = (_, c, _, _) => c is not null,
                    });
                    ClientSubject = ssl.RemoteCertificate?.Subject;
                    AuthStart = Read(ssl);
                    if (deny is not null)
                    {
                        var w = new BinWriter();
                        w.U8(13); w.U8(0); w.Str(deny);
                        Send(ssl, w.ToArray());
                        return;
                    }
                    var ok = new byte[34];
                    ok[0] = 13; ok[1] = 1;
                    Send(ssl, ok);
                    while (Read(ssl) is not null) Send(ssl, F.Ddl());
                }
                catch (Exception e)
                {
                    Error = e.GetType().Name + ": " + e.Message;
                }
            }) { IsBackground = true };
            _thread.Start();
        }

        private static byte[]? Read(Stream s)
        {
            var head = new byte[4];
            if (!Fill(s, head)) return null;
            var body = new byte[(head[0] << 24) | (head[1] << 16) | (head[2] << 8) | head[3]];
            return Fill(s, body) ? body : null;
        }

        private static bool Fill(Stream s, byte[] buf)
        {
            int got = 0;
            while (got < buf.Length)
            {
                int n;
                try { n = s.Read(buf, got, buf.Length - got); }
                catch (IOException) { return false; }
                if (n <= 0) return false;
                got += n;
            }
            return true;
        }

        private static void Send(Stream s, byte[] payload)
        {
            s.Write(F.Frame(payload));
            s.Flush();
        }

        public void Join()
        {
            _thread.Join(TimeSpan.FromSeconds(10));
            _listener.Stop();
        }

        public void Dispose() => _listener.Stop();
    }

    private string Cs(TlsServer srv, string extra) =>
        $"Host=127.0.0.1;Port={srv.Port};TlsCa={_caPem};AuthMechanism=Certificate;Timeout=5;{extra}";

    [Fact]
    public void A_default_user_sends_an_empty_username_and_the_certificate_is_the_login()
    {
        using var srv = new TlsServer(_serverCert, deny: null);
        using (var c = new SkaidbConnection(Cs(srv, $"TlsClientCert={_clientPem};TlsClientKey={_clientKey}")))
        {
            Assert.Equal("anonymous", c.User);                                  // the driver default ...
            c.Open();
            Assert.True(c.IsOpen);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (PRIMARY KEY (id))";
            Assert.Equal(0, cmd.ExecuteNonQuery());                             // the session works
        }
        srv.Join();
        Assert.Null(srv.Error);
        Assert.Equal("CN=ada", srv.ClientSubject);                              // the certificate was presented
        // ... is NOT sent: an empty name lets the certificate's CN decide.
        Assert.Equal(SkaidbConnection.BuildAuthStart("", "", SkaidbAuthMechanism.Certificate), srv.AuthStart);
    }

    [Fact]
    public void An_explicit_user_is_asserted_and_a_combined_PEM_works()
    {
        using var srv = new TlsServer(_serverCert, deny: null);
        using (var c = new SkaidbConnection(Cs(srv, $"Username=ada;TlsClientCert={_clientCombined}")))
        {
            c.Open();
            Assert.True(c.IsOpen);
        }
        srv.Join();
        Assert.Null(srv.Error);
        Assert.Equal("CN=ada", srv.ClientSubject);
        Assert.Equal(SkaidbConnection.BuildAuthStart("ada", "", SkaidbAuthMechanism.Certificate), srv.AuthStart);
    }

    [Fact]
    public void A_denied_certificate_fails_the_connect_with_the_reason()
    {
        using var srv = new TlsServer(_serverCert, deny: "certificate user 'ada' does not match 'bob'");
        using var c = new SkaidbConnection(Cs(srv, $"Username=bob;TlsClientCert={_clientPem};TlsClientKey={_clientKey}"));
        var e = Assert.Throws<SkaidbException>(() => c.Open());
        Assert.Contains("authentication denied: certificate user 'ada' does not match 'bob'", e.Message);
        Assert.False(c.IsOpen);
        srv.Join();
        Assert.Null(srv.Error);
    }
}
