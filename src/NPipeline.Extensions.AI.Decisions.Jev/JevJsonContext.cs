using System.Text.Json.Serialization;

namespace NPipeline.Extensions.AI.Decisions.Jev;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(JevSystemOneRequest))]
[JsonSerializable(typeof(JevSystemOneResponse))]
internal sealed partial class JevJsonContext : JsonSerializerContext;
