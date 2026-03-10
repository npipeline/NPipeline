using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_HttpConnector.Models;

namespace Sample_HttpConnector.Nodes;

/// <summary>
///     Formats a <see cref="GithubRelease" /> into a <see cref="SlackMessage" />.
/// </summary>
public sealed class ReleaseFormatter : TransformNode<GithubRelease, SlackMessage>
{
    public override Task<SlackMessage> TransformAsync(
        GithubRelease item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var releaseType = item.Prerelease
            ? "pre-release"
            : "release";

        var summary = item.Body?.Length > 200
            ? string.Concat(item.Body.AsSpan(0, 200), "…")
            : item.Body ?? "";

        var text = $"*New {releaseType}: {item.TagName}*\n" +
                   $"{item.Name ?? item.TagName}\n\n" +
                   $"{summary}\n\n" +
                   $"<{item.HtmlUrl}|View on GitHub>";

        return Task.FromResult(new SlackMessage { Text = text });
    }
}
