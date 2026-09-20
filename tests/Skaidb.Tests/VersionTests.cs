// The package version is defined once, in Skaidb.csproj. These tests pin the
// version the driver reports to the server in the Hello frame to it.

using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;

namespace Skaidb.Tests;

public class VersionTests
{
    /// <summary>The &lt;Version&gt; in Skaidb.csproj, found by walking up from the test binary.</summary>
    public static string CsprojVersion()
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null)
        {
            string p = Path.Combine(dir, "Skaidb.csproj");
            if (File.Exists(p))
            {
                var m = Regex.Match(File.ReadAllText(p), @"<Version>([^<]+)</Version>");
                Assert.True(m.Success, "Skaidb.csproj has no <Version>");
                return m.Groups[1].Value;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new InvalidOperationException("Skaidb.csproj not found above " + AppContext.BaseDirectory);
    }

    [Fact]
    public void DriverVersion_equals_the_csproj_Version_and_is_semver()
    {
        Assert.Matches(@"^\d+\.\d+\.\d+$", SkaidbConnection.DriverVersion);
        Assert.Equal(CsprojVersion(), SkaidbConnection.DriverVersion);
        Assert.Equal("dotnet", SkaidbConnection.DriverName);
    }

    [Fact]
    public void Source_has_no_version_literal()
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Skaidb.cs"))) dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        string src = File.ReadAllText(Path.Combine(dir!, "Skaidb.cs"));
        Assert.DoesNotMatch(@"""\d+\.\d+\.\d+""", src);
        Assert.Contains("AssemblyInformationalVersionAttribute", src);
    }

    [Fact]
    public void Hello_frame_carries_the_package_version()
    {
        using var srv = new FakeServer();
        using var c = new SkaidbConnection($"Host=127.0.0.1;Port={srv.Port};User=ada;Password=secret");
        c.Open();
        Assert.Single(srv.Hellos);
        Assert.Equal("dotnet", srv.Hellos[0].Name);
        Assert.Equal(CsprojVersion(), srv.Hellos[0].Version);
        // Exactly: u8 8, str name, str version, nothing trailing.
        byte[] raw = srv.Hellos[0].Raw;
        Assert.Equal(8, raw[0]);
        Assert.Equal(1 + 4 + 6 + 4 + SkaidbConnection.DriverVersion.Length, raw.Length);
    }
}
