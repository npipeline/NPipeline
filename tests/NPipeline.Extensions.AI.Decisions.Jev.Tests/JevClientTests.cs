using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev.Tests;

public sealed class JevClientTests
{
    private const string SuccessJson = """
                                       {
                                         "model": "jev-1.13.0",
                                         "answers": {
                                           "department": {
                                             "type": "choice",
                                             "choice": "billing",
                                             "confidence": 0.8,
                                             "probabilities": { "billing": 0.9, "technical": 0.1 }
                                           },
                                           "urgent": { "type": "noul", "noul": 0.7 },
                                           "complexity": {
                                             "type": "score",
                                             "score": 0.4,
                                             "confidence": 0.6,
                                             "legend": { "0": "Simple", "1": "Complex" },
                                             "probabilities": { "0": 0.6, "1": 0.4 }
                                           }
                                         },
                                         "usage": { "input_tokens": 42, "output_tokens": 12 }
                                       }
                                       """;

    [Fact]
    public async Task EvaluateAsync_SendsSystemOneRequestAndReadsMetadata()
    {
        string? requestBody = null;
        AuthenticationHeaderValue? authorization = null;
        Uri? requestUri = null;

        var handler = new DelegateHandler(async (request, cancellationToken) =>
        {
            requestBody = await request.Content!.ReadAsStringAsync(cancellationToken);
            authorization = request.Headers.Authorization;
            requestUri = request.RequestUri;
            var response = JsonResponse(HttpStatusCode.OK, SuccessJson);
            response.Headers.Add("x-typesafe-request-id", "req-123");
            return response;
        });

        var client = CreateClient(handler);
        var state = new JsonObject { ["message"] = "charged twice" };

        var questions = new Dictionary<string, JevQuestion>
        {
            ["department"] = new JevChoiceQuestion(
                "Which team should handle this?",
                new Dictionary<string, string?> { ["billing"] = "Payments", ["technical"] = "Bugs" }),
            ["urgent"] = new JevNoulQuestion("Is this urgent?"),
            ["complexity"] = new JevScoreQuestion("How complex is this?", ["Simple", "Complex"]),
        };

        var response = await client.EvaluateAsync(state, questions);

        Assert.Equal(new Uri("https://api.typesafe.ai/v1/systemone"), requestUri);
        Assert.Equal("Bearer", authorization?.Scheme);
        Assert.Equal("secret", authorization?.Parameter);
        Assert.NotNull(requestBody);
        var sent = JsonNode.Parse(requestBody!)!.AsObject();
        Assert.Equal("jev-latest", sent["model"]!.GetValue<string>());
        Assert.Equal("charged twice", sent["state"]!["message"]!.GetValue<string>());
        Assert.Equal("choice", sent["questions"]!["department"]!["type"]!.GetValue<string>());
        Assert.Equal("noul", sent["questions"]!["urgent"]!["type"]!.GetValue<string>());
        Assert.Equal("score", sent["questions"]!["complexity"]!["type"]!.GetValue<string>());
        Assert.IsType<JevChoiceAnswer>(response.Answers["department"]);
        Assert.Equal("jev-1.13.0", response.Model);
        Assert.Equal("req-123", response.RequestId);
        Assert.Equal(42, response.Usage.InputTokens);
    }

    [Fact]
    public async Task EvaluateAsync_RetriesRateLimitAndHonorsRetryAfterMilliseconds()
    {
        var attempts = 0;

        var handler = new DelegateHandler((_, _) =>
        {
            attempts++;

            if (attempts == 1)
            {
                var limited = JsonResponse(HttpStatusCode.TooManyRequests, "{}");
                limited.Headers.Add("retry-after-ms", "0");
                return Task.FromResult(limited);
            }

            return Task.FromResult(JsonResponse(HttpStatusCode.OK, SuccessJson));
        });

        var client = CreateClient(handler, new JevRetryPolicy { MaxRetries = 1, InitialDelay = TimeSpan.Zero, JitterFactor = 0 });

        await client.EvaluateAsync(JsonValue.Create("state"), ChoiceQuestions());

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task EvaluateAsync_DoesNotRetryValidationFailure()
    {
        var attempts = 0;

        var handler = new DelegateHandler((_, _) =>
        {
            attempts++;
            var response = JsonResponse(HttpStatusCode.UnprocessableEntity, "{\"detail\":\"bad question\"}");
            response.Headers.Add("x-typesafe-request-id", "req-error");
            return Task.FromResult(response);
        });

        var client = CreateClient(handler);

        var exception = await Assert.ThrowsAsync<JevApiException>(async () =>
            await client.EvaluateAsync(JsonValue.Create("state"), ChoiceQuestions()));

        Assert.Equal(HttpStatusCode.UnprocessableEntity, exception.StatusCode);
        Assert.Equal("req-error", exception.RequestId);
        Assert.Contains("bad question", exception.ResponseBody, StringComparison.Ordinal);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task EvaluateAsync_AttemptTimeoutThrowsTimeoutException()
    {
        var handler = new DelegateHandler(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return JsonResponse(HttpStatusCode.OK, SuccessJson);
        });

        using var httpClient = new HttpClient(handler);

        var client = new JevClient(httpClient, new JevClientOptions
        {
            ApiKey = "secret",
            AttemptTimeout = TimeSpan.FromMilliseconds(20),
            Retry = new JevRetryPolicy { MaxRetries = 0 },
        });

        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await client.EvaluateAsync(JsonValue.Create("state"), ChoiceQuestions()));
    }

    private static JevClient CreateClient(DelegateHandler handler, JevRetryPolicy? retry = null) =>
        new(new HttpClient(handler), new JevClientOptions
        {
            ApiKey = "secret",
            Retry = retry ?? new JevRetryPolicy(),
        });

    private static IReadOnlyDictionary<string, JevQuestion> ChoiceQuestions() =>
        new Dictionary<string, JevQuestion>
        {
            ["department"] = new JevChoiceQuestion(
                "Which team?",
                new Dictionary<string, string?> { ["billing"] = null, ["technical"] = null }),
        };

    private static HttpResponseMessage JsonResponse(HttpStatusCode statusCode, string json) =>
        new(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            send(request, cancellationToken);
    }
}
