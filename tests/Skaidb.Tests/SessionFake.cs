// An in-process server that keeps what a real one keeps per connection: the
// session. Its database, its consistency override, its scan budget and its
// open transaction live and die with the socket, so a test can assert where,
// and under which settings, a statement actually ran.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Skaidb.Tests;

/// <summary>
/// A fake standalone server with sessions. A connection starts in the
/// database <c>default</c>, with no consistency override, no scan budget and
/// no transaction. It understands
/// <list type="bullet">
/// <item><c>USE [DATABASE] name</c> (an Error for a database it does not have, or that is <see cref="Dropped"/>);</item>
/// <item><c>SET CONSISTENCY ONE|QUORUM|ALL|DEFAULT</c> — the override every later statement runs at; <c>DEFAULT</c> is a parse error on an <see cref="Old"/> server;</item>
/// <item><c>SET SCAN BUDGET ROWS n [BYTES n]</c>, <c>BYTES n [ROWS n]</c>, <c>DEFAULT</c>;</item>
/// <item><c>BEGIN [TRANSACTION]</c>, <c>COMMIT [TRANSACTION]</c>, <c>ROLLBACK [TRANSACTION]</c>, answered as a real server answers them with and without a transaction open;</item>
/// <item><c>SELECT count(*) FROM notes</c>, <c>SELECT id FROM notes</c> (a scan: refused past the session's row budget) and <c>INSERT INTO notes (id, body) VALUES (n, …)</c> against the table <c>notes</c> of the session's database. An insert inside a transaction is seen by its own session and by nobody else until the COMMIT, and is gone when the connection closes first.</item>
/// </list>
/// <c>--</c> comments are skipped, a trailing <c>;</c> is allowed, a text
/// with a second statement behind a <c>;</c> is answered Ddl and does
/// nothing. Statements with <c>?</c> placeholders can be prepared when they
/// are a SELECT or an INSERT; session statements cannot, as on a real
/// server. Everything else is answered Ddl. <see cref="Log"/> has every
/// statement with the session it ran in.
/// </summary>
public sealed class Sessions
{
    /// <summary>One statement as the server ran it.</summary>
    /// <param name="Sql">The text (of a prepared statement: its text).</param>
    /// <param name="Database">The session's database when it arrived.</param>
    /// <param name="Level">The level it ran at: the session's override, else the request's own.</param>
    /// <param name="Budget">The session's scan budget in rows, or null.</param>
    /// <param name="InTransaction">Whether the session had a transaction open.</param>
    /// <param name="Socket">The connection it arrived on.</param>
    public sealed record Ran(string Sql, string Database, string Level, long? Budget, bool InTransaction, FakeConnection Socket);

    private sealed class Session
    {
        public string Database = "default";
        public string? Level;
        public long? Budget;
        public List<(string Database, long Id)>? Pending;
    }

    private static readonly Regex Use = new(
        @"^USE\s+(?:DATABASE\s+)?(?:""((?:[^""]|"""")+)""|(\w+))$", RegexOptions.IgnoreCase);
    private static readonly Regex Insert = new(
        @"^INSERT\s+INTO\s+notes\s*\(\s*id\s*,\s*body\s*\)\s*VALUES\s*\(\s*(\d+|\?)\s*,", RegexOptions.IgnoreCase);
    private static readonly string[] Levels = { "ONE", "QUORUM", "ALL" };

    private readonly object _gate = new();
    private readonly Dictionary<FakeConnection, Session> _sessions = new();
    private readonly List<Ran> _log = new();
    private readonly Dictionary<uint, string> _ids = new();
    private uint _nextId = 1;
    private readonly Dictionary<string, SortedSet<long>> _notes = new()
    {
        ["default"] = new SortedSet<long>(),
        ["app"] = new SortedSet<long> { 1, 2, 3 },
        ["other"] = new SortedSet<long> { 1, 2, 3, 4, 5, 6, 7 },
    };

    /// <summary>A database that no longer exists: USE of it is refused.</summary>
    public volatile string? Dropped;

    /// <summary>A server before 0.349: <c>SET CONSISTENCY DEFAULT</c> is a parse error.</summary>
    public volatile bool Old;

    /// <summary>
    /// Refuse a statement: the error to answer it with, or null to run it.
    /// Asked with the text and the connection it arrived on. A refused
    /// COMMIT ends the session's transaction, as on a real server.
    /// </summary>
    public Func<string, FakeConnection, string?>? Refuse;

    /// <summary>Close the socket instead of answering this statement (it is logged, and has not run).</summary>
    public Func<string, bool>? Drop;

    public Ran[] Log { get { lock (_gate) return _log.ToArray(); } }

    /// <summary>The texts logged from position <paramref name="mark"/> on.</summary>
    public string[] Since(int mark) { lock (_gate) return _log.Skip(mark).Select(r => r.Sql).ToArray(); }

    /// <summary>Committed ids of <c>notes</c> in <paramref name="database"/>.</summary>
    public long[] Ids(string database) { lock (_gate) return _notes[database].ToArray(); }

    public int Rows(string database) { lock (_gate) return _notes[database].Count; }

    private static string SkipComments(string sql)
    {
        var kept = new System.Text.StringBuilder();
        bool inName = false, inString = false;
        foreach (string line in sql.Split('\n'))
        {
            int end = line.Length;
            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == '"' && !inString) inName = !inName;
                else if (line[i] == '\'' && !inName) inString = !inString;
                else if (!inName && !inString && line[i] == '-' && i + 1 < line.Length && line[i + 1] == '-')
                {
                    end = i;
                    break;
                }
            }
            kept.Append(line, 0, end).Append('\n');
        }
        return kept.ToString();
    }

    public IEnumerable<byte[]>? Handle(Request req, FakeConnection conn)
    {
        lock (_gate)
        {
            switch (req.Op)
            {
                case 1:
                    return Statement(req.Sql, null, conn, req.Consistency, streamed: false);
                case 5:
                    return Statement(req.Sql, null, conn, req.Consistency, streamed: true);
                case 9:
                    return Statement(req.Sql, req.Params, conn, req.Consistency, streamed: true);
                case 2:
                {
                    string head = SkipComments(req.Sql).TrimStart();
                    if (!head.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                        && !head.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
                        return new[] { F.Error("statement kind cannot be prepared") };
                    uint id = _nextId++;
                    _ids[id] = req.Sql;
                    return new[] { F.Prepared(id, (ushort)req.Sql.Count(ch => ch == '?')) };
                }
                case 3:
                    return Statement(_ids[req.Id], req.Params, conn, req.Consistency, streamed: false);
                case 4:
                    return new[] { F.Ddl() };
                case 7:
                {
                    ulong total = 0;
                    foreach (var row in req.Rows)
                    {
                        var answer = Statement(_ids[req.Id], row, conn, req.Consistency, streamed: false).ToArray();
                        if (answer.Length == 1 && answer[0][0] == 3) return answer;
                        total++;
                    }
                    return new[] { F.Mutation(total) };
                }
                default:
                    return null;   // Hello -> Ddl
            }
        }
    }

    private IEnumerable<byte[]> Statement(string sql, List<object?>? values, FakeConnection conn, byte level, bool streamed)
    {
        if (!_sessions.TryGetValue(conn, out var session)) _sessions[conn] = session = new Session();
        _log.Add(new Ran(sql, session.Database, session.Level ?? Levels[level], session.Budget, session.Pending is not null, conn));

        string text = SkipComments(sql).Trim();
        if (text.EndsWith(';')) text = text.Substring(0, text.Length - 1).TrimEnd();
        string[] words = text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        string first = words.Length > 0 ? words[0].ToUpperInvariant() : "";
        bool Word(int i, string w) => words.Length > i && words[i].Equals(w, StringComparison.OrdinalIgnoreCase);

        if (Drop?.Invoke(sql) == true)
        {
            conn.Sock.Shutdown(System.Net.Sockets.SocketShutdown.Both);
            conn.Sock.Close();
            return Array.Empty<byte[]>();
        }
        if (Refuse?.Invoke(sql, conn) is string refused)
        {
            if (first == "COMMIT") session.Pending = null;
            return new[] { F.Error(refused) };
        }
        if (text.Contains(';')) return new[] { F.Ddl() };          // not one statement: nothing of it is applied

        switch (first)
        {
            case "USE":
            {
                var use = Use.Match(text);
                if (!use.Success) return new[] { F.Error("parse error: expected a database name") };
                string name = use.Groups[1].Success ? use.Groups[1].Value.Replace("\"\"", "\"") : use.Groups[2].Value;
                if (!_notes.ContainsKey(name) || name == Dropped)
                    return new[] { F.Error($"database \"{name}\" does not exist") };
                session.Database = name;
                return new[] { F.Ddl() };
            }
            case "SET" when Word(1, "CONSISTENCY"):
            {
                string asked = words.Length > 2 ? words[2].ToUpperInvariant() : "";
                if (asked == "DEFAULT" && !Old) session.Level = null;
                else if (Array.IndexOf(Levels, asked) >= 0) session.Level = asked;
                else
                    return new[]
                    {
                        F.Error(Old ? "parse error: SET CONSISTENCY takes ONE, QUORUM, or ALL"
                                    : "parse error: SET CONSISTENCY takes ONE, QUORUM, ALL, or DEFAULT"),
                    };
                return new[] { F.Ddl() };
            }
            case "SET" when Word(1, "SCAN") && Word(2, "BUDGET"):
            {
                if (Word(3, "DEFAULT") && words.Length == 4)
                {
                    session.Budget = null;
                    return new[] { F.Ddl() };
                }
                long? rows = null;
                bool any = false;
                for (int i = 3; i + 1 < words.Length + 1; i += 2)
                {
                    if (i + 1 >= words.Length || !long.TryParse(words[i + 1], out long n) || n <= 0
                        || !(Word(i, "ROWS") || Word(i, "BYTES")))
                        return new[] { F.Error("parse error: expected SET SCAN BUDGET: ROWS <n>, BYTES <n>, or DEFAULT") };
                    if (Word(i, "ROWS")) rows = n;
                    any = true;
                }
                if (!any) return new[] { F.Error("parse error: expected SET SCAN BUDGET: ROWS <n>, BYTES <n>, or DEFAULT") };
                session.Budget = rows;
                return new[] { F.Ddl() };
            }
            case "BEGIN":
                if (session.Pending is not null)
                    return new[] { F.Error("constraint violation: a transaction is already in progress") };
                session.Pending = new List<(string, long)>();
                return new[] { F.Ddl() };
            case "COMMIT" when words.Length == 1 || (words.Length == 2 && Word(1, "TRANSACTION")):
                if (session.Pending is null)
                    return new[] { F.Error("constraint violation: no transaction in progress") };
                foreach (var (database, id) in session.Pending) _notes[database].Add(id);
                session.Pending = null;
                return new[] { F.Ddl() };
            case "ROLLBACK" when words.Length == 1 || (words.Length == 2 && Word(1, "TRANSACTION")):
                session.Pending = null;                              // success with nothing open, too
                return new[] { F.Ddl() };
        }

        // The rows this session sees: the committed ones, and its own.
        var seen = new SortedSet<long>(_notes[session.Database]);
        if (session.Pending is not null)
            foreach (var (database, id) in session.Pending)
                if (database == session.Database) seen.Add(id);

        if (text.StartsWith("SELECT count(*) FROM notes", StringComparison.OrdinalIgnoreCase))
            return new[] { F.Rows(new[] { "count" }, new object?[] { (long)seen.Count }) };
        if (text.StartsWith("SELECT id FROM notes", StringComparison.OrdinalIgnoreCase))
        {
            if (session.Budget is long budget && seen.Count > budget)
                return new[]
                {
                    F.Error($"resource limit: scan budget exceeded ({budget + 1} rows examined): narrow the filter, "
                            + $"add a covering index, or raise storage.scan_row_budget [table {session.Database}.notes]"),
                };
            var rows = seen.Select(id => new object?[] { id }).ToArray();
            return streamed
                ? new[] { F.Header("id"), F.Chunk(rows), F.End() }
                : new[] { F.Rows(new[] { "id" }, rows) };
        }
        var insert = Insert.Match(text);
        if (insert.Success)
        {
            long id = insert.Groups[1].Value == "?" ? Convert.ToInt64(values![0]) : long.Parse(insert.Groups[1].Value);
            if (session.Pending is not null) session.Pending.Add((session.Database, id));
            else _notes[session.Database].Add(id);
            return new[] { F.Mutation(1) };
        }
        if (first == "SELECT") return new[] { F.Rows(new[] { "v" }, new object?[] { 1L }) };
        return new[] { F.Ddl() };
    }
}
