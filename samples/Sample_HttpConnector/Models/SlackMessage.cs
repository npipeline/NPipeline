using System.Text.Json.Serialization;

namespace Sample_HttpConnector.Models;

public sealed class SlackMessage
{
    [JsonPropertyName("text")]
    public string Text { get; init; } = "";
}
