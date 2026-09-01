using System.Text;
using System.Text.RegularExpressions;
using Serilog;

namespace VRCVideoCacher.Utils;

/// <summary>
/// Reads the instance the player is currently in out of VRChat's own log.
///
/// VRChat writes a <c>[Behaviour] Joining wrld_…</c> line on every instance change, so the last one in
/// the newest log is where the player is right now. There is no API for this — VRChat does not tell a
/// yt-dlp call which world asked for the video — and the log is the only signal available locally.
/// </summary>
public static class VrChatLog
{
    private static readonly ILogger Log = Program.Logger.ForContext(typeof(VrChatLog));

    private static readonly Regex JoinRegex = new(@"Joining (wrld_[0-9a-fA-F-]{36})", RegexOptions.Compiled);

    /// <summary>
    /// Only the tail is read. These logs reach megabytes over a session and the join line we want is
    /// near the end, so scanning the whole file would cost far more than the answer is worth.
    /// </summary>
    private const int TailBytes = 512 * 1024;

    /// <summary>
    /// The world id of the current instance, or null when it cannot be determined — no log directory, no
    /// logs, logging disabled, or nothing joined yet. Callers must treat null as "unknown" and take the
    /// safe branch rather than assuming any particular world.
    /// </summary>
    public static string? GetCurrentWorldId()
    {
        try
        {
            var dir = FileTools.VrChatDataPath;
            if (string.IsNullOrEmpty(dir) || !Directory.Exists(dir))
                return null;

            FileInfo? newest = null;
            foreach (var file in new DirectoryInfo(dir).GetFiles("output_log_*.txt"))
                if (newest is null || file.LastWriteTimeUtc > newest.LastWriteTimeUtc)
                    newest = file;
            if (newest is null)
                return null;

            // VRChat keeps the log open for writing, so share everything — including delete, since it
            // rotates logs while running.
            using var stream = new FileStream(newest.FullName, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            if (stream.Length > TailBytes)
                stream.Seek(-TailBytes, SeekOrigin.End);

            using var reader = new StreamReader(stream, Encoding.UTF8);
            string? line;
            string? world = null;
            while ((line = reader.ReadLine()) is not null)
            {
                var match = JoinRegex.Match(line);
                if (match.Success)
                    world = match.Groups[1].Value;
            }

            return world;
        }
        catch (Exception ex)
        {
            Log.Debug(ex, "Could not read the current world from VRChat's log");
            return null;
        }
    }
}
