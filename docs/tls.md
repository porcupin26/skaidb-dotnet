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

`TlsClientCert` / `TlsClientKey` (PEM files) make the driver present a
client certificate in the TLS handshake, which a server with
`client_tls = "required"` and client verification needs. The key may live in
the certificate file; then leave `TlsClientKey` out. Either key implies
`Tls`. On its own the certificate only opens the TLS session; the login is
still the SCRAM user.

With `AuthMechanism=Certificate` the certificate IS the login (wire
mechanism EXTERNAL): the server maps its Common Name to a role and no
password is sent. The server needs `auth.x509_enabled`, and its client CA
must have signed the certificate.

```csharp
new SkaidbConnection("Host=db1;TlsCa=/etc/skaidb/ca.crt;" +
                     "TlsClientCert=/etc/app/app.crt;TlsClientKey=/etc/app/app.key;" +
                     "AuthMechanism=Certificate;Database=app");
```

Give `Username` only to assert the expected identity: it must equal the
certificate's CN, and a certificate mapped to a different role fails the
connect. Without it the driver sends an empty name (not the `anonymous`
default) and the CN decides. `AuthMechanism=Certificate` without
`TlsClientCert` fails before dialing.

## Failure modes

| Symptom | Cause |
|---|---|
| `connect failed: Authentication failed …` / `The remote certificate is invalid …` | The certificate does not verify: wrong CA, or `TlsServerName` does not match a SAN. |
| `connect failed: … handshake` on a server without TLS | TLS was requested on a plaintext listener. Drop the TLS keys. |
| `connection closed by server` during `Open()` without TLS | The server has `client_tls = required`. Add `TlsCa`. |
| `connect failed: Could not find file '/etc/skaidb/ca.crt'` | `TlsCa` path unreadable. |
| `authentication denied: …` with `AuthMechanism=Certificate` | The server does not accept the certificate as a login: `auth.x509_enabled` is off, its CN maps to no role, or `Username` differs from the CN. |

The example in [`examples/Tls`](../examples/Tls/Program.cs) opens a verified
connection and lists the databases.
