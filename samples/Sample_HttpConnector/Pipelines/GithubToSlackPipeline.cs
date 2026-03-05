using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Http.Auth;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Pagination;
using NPipeline.Pipeline;
using Sample_HttpConnector.Models;
using Sample_HttpConnector.Nodes;

namespace Sample_HttpConnector.Pipelines;

/// <summary>
///     Pipeline 1: Fetch GitHub releases and post formatted summaries to a Slack webhook.
///     <para>
///         Flow: <c>HttpSourceNode&lt;GithubRelease&gt;</c>
///         → <c>ReleaseFormatter</c>
///         → <c>HttpSinkNode&lt;SlackMessage&gt;</c>
///     </para>
/// </summary>
public sealed class GithubToSlackPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<HttpSourceNode<GithubRelease>, GithubRelease>("github-source");
        var format = builder.AddTransform<ReleaseFormatter, GithubRelease, SlackMessage>("slack-formatter");
        var sink = builder.AddSink<HttpSinkNode<SlackMessage>, SlackMessage>("slack-sink");

        builder.Connect(source, format);
        builder.Connect(format, sink);
    }

    public static string GetDescription()
    {
        return """
               GitHub Releases → Slack Webhook

               Fetches the latest releases from a GitHub repository using link-header pagination
               and posts a formatted summary to a Slack incoming webhook.

               Required environment variables:
                 GITHUB_TOKEN  — personal access token (or fine-grained PAT) with repo read access
                 SLACK_WEBHOOK — Slack incoming webhook URL

               Optional:
                 GITHUB_OWNER  — repository owner    (default: dotnet)
                 GITHUB_REPO   — repository name     (default: runtime)
               """;
    }

    /// <summary>Registers the DI services needed by this pipeline.</summary>
    public static void RegisterServices(IServiceCollection services)
    {
        var githubToken = Environment.GetEnvironmentVariable("GITHUB_TOKEN") ?? "";
        var slackWebhook = Environment.GetEnvironmentVariable("SLACK_WEBHOOK") ?? "https://httpbin.org/post";
        var owner = Environment.GetEnvironmentVariable("GITHUB_OWNER") ?? "dotnet";
        var repo = Environment.GetEnvironmentVariable("GITHUB_REPO") ?? "runtime";

        // GitHub source configuration
        services.AddSingleton(new HttpSourceConfiguration
        {
            BaseUri = new Uri($"https://api.github.com/repos/{owner}/{repo}/releases"),
            Headers = new Dictionary<string, string>
            {
                ["User-Agent"] = "NPipeline-Sample/1.0",
                ["Accept"] = "application/vnd.github+json",
                ["X-GitHub-Api-Version"] = "2022-11-28",
            },
            Auth = string.IsNullOrEmpty(githubToken)
                ? NullAuthProvider.Instance
                : new BearerTokenAuthProvider(githubToken),
            Pagination = new LinkHeaderPaginationStrategy(),
            MaxPages = 3, // Limit to avoid exhausting API rate limit in a sample
        });

        // Slack sink configuration
        services.AddSingleton(new HttpSinkConfiguration
        {
            Uri = new Uri(slackWebhook),
            Method = SinkHttpMethod.Post,
            BatchSize = 1, // One message per release
        });

        // HTTP client
        services.AddHttpClient();
    }
}
