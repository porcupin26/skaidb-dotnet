# TLS

The binary protocol can run inside TLS. The handshake happens right after the
TCP connect; SCRAM authentication and every request then ride inside the TLS
session. The server side (listener certificate, `client_tls` policy, the
cluster CA) is covered in the server docs at <https://skaidb.org/docs/>.

## Three modes

```csharp
// 1. Verify against the system trust store (a publicly trusted or
//    OS-installed CA). SNI and the expected name are TlsServerName.
new SkaidbConnection("Host=db1.example.com;Tls=true;TlsServerName=db1.example.com;User=app;Password=pw");

// 2. Verify against a specific CA — the cluster's own ca.crt. This is the
//    usual production shape.
new SkaidbConnection("Host=db1;TlsCa=/etc/skaidb/ca.crt;User=app;Password=pw");

// 3. No verification at all (self-signed dev server). INSECURE: any peer
//    can impersonate the server.
new SkaidbConnection("Host=db1;TlsInsecure=true;User=app;Password=pw");
```

Setting any of `Tls=true`, `TlsCa=…` or `TlsInsecure=true` enables TLS
(`Tls` reads true afterwards). `TlsInsecure` wins over `TlsCa`. Without any
of them the connection is plaintext, even if the server would accept TLS. The
named-argument constructor takes the same options (`tls`, `tlsCa`,
`tlsInsecure`, `tlsServerName`).

## The server name

`TlsServerName` (default `skaidb`) is sent as SNI and is the name the server
certificate must match under modes 1 and 2. skaidb's generated certificates
carry `skaidb` as a subject alternative name, which is why the default is not
the host you dialled: the same certificate is valid on every node whatever
its address. If your certificates instead carry real hostnames, pass the
hostname.

## How verification works

Mode 1 uses `SslStream` with the platform's default validation. Mode 2 keeps
the default validation and, when it fails only because the issuer is unknown,
rebuilds the chain with `X509ChainTrustMode.CustomRootTrust` over the
certificates in `TlsCa` (a PEM bundle; several certificates are fine) with
revocation checking off. The name check is the platform's, against
`TlsServerName`.

## Pools and seeds

TLS keys pass through `SkaidbConnectionPool` and apply to every seed:

```csharp
new SkaidbConnectionPool("Seeds=db1:7000,db2:7000,db3:7000;TlsCa=/etc/skaidb/ca.crt;User=app;Password=pw;Database=app");
```

## Client certificates

The driver does not present a client certificate; identity is the SCRAM
user. A server configured to require client certificates on the binary port
will reject this driver's handshake.

## Failure modes

| Symptom | Cause |
|---|---|
| `connect failed: Authentication failed …` / `The remote certificate is invalid …` | The certificate does not verify: wrong CA, or `TlsServerName` does not match a SAN. |
| `connect failed: … handshake` on a server without TLS | TLS was requested on a plaintext listener. Drop the TLS keys. |
| `connection closed by server` during `Open()` without TLS | The server has `client_tls = required`. Add `TlsCa`. |
| `connect failed: Could not find file '/etc/skaidb/ca.crt'` | `TlsCa` path unreadable. |

The example in [`examples/Tls`](../examples/Tls/Program.cs) opens a verified
connection and lists the databases.
