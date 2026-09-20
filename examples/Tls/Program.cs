// Connect over TLS.
//
//   dotnet run --project examples/Tls -- host port user password [ca.crt] [server-name]
//
// With a CA file the server certificate is verified against that CA (the
// cluster's own ca.crt) and the SNI / expected name is TlsServerName
// (default "skaidb" — skaidb's own certificates carry DNS:skaidb, which is
// usually NOT the address you dialled). Without a CA file the system trust
// store is used. For a self-signed dev server only, TlsInsecure=true skips
// verification: it encrypts but authenticates nothing.

using System;
using System.Globalization;
using Skaidb;

if (args.Length < 4)
{
    Console.Error.WriteLine("usage: host port user password [ca.crt] [server-name]");
    return 2;
}
string host = args[0];
int port = int.Parse(args[1], CultureInfo.InvariantCulture);
string user = args[2], password = args[3];
string ca = args.Length > 4 ? args[4] : "";
string serverName = args.Length > 5 ? args[5] : "skaidb";

string connString = ca.Length > 0
    ? $"Host={host};Port={port};User={user};Password={password};TlsCa={ca};TlsServerName={serverName}"
    : $"Host={host};Port={port};User={user};Password={password};Tls=true;TlsServerName={serverName}";
// Dev only, no verification:  ...;TlsInsecure=true

using var conn = new SkaidbConnection(connString);
conn.Open();
Console.WriteLine($"TLS connection to {host}:{port} established (verified as '{serverName}').");

using var cmd = conn.CreateCommand();
cmd.CommandText = "SHOW DATABASES";
using var r = cmd.ExecuteReader();
while (r.Read()) Console.WriteLine("  " + r.GetValue(0));
return 0;
