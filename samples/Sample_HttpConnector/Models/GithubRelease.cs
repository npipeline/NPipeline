using System.Text.Json.Serialization;

namespace Sample_HttpConnector.Models;

public sealed class GithubRelease
{
    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("tag_name")]
    public string TagName { get; init; } = "";

    [JsonPropertyName("name")]
    public string? Name { get; init; }

    [JsonPropertyName("body")]
    public string? Body { get; init; }

    [JsonPropertyName("published_at")]
    public DateTimeOffset PublishedAt { get; init; }

    [JsonPropertyName("html_url")]
    public string HtmlUrl { get; init; } = "";

    [JsonPropertyName("prerelease")]
    public bool Prerelease { get; init; }
}
