using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev.Tests;

public sealed class JevChoiceClassifierTests
{
    [Fact]
    public async Task ClassifyAsync_MapsTypedLabelsAndMetadata()
    {
        var client = new FakeJevClient(Response(
            new JevChoiceAnswer("technical", 0.7, new Dictionary<string, double>
            {
                ["billing"] = 0.3,
                ["technical"] = 0.7,
            })));

        var options = new JevChoiceClassifierOptionsBuilder<Ticket, RouteLabel>()
            .WithState(ticket => new { ticket.Message })
            .WithInstructions("Which team?")
            .AddChoice(RouteLabel.Billing, "billing", "Payments")
            .AddChoice(RouteLabel.Technical, "technical", "Bugs")
            .WithModel("jev-1.13.0")
            .Build();

        var classifier = new JevChoiceClassifier<Ticket, RouteLabel>(client, options);

        var result = await classifier.ClassifyAsync(new Ticket("site is down"));

        Assert.Equal(RouteLabel.Technical, result.Label);
        Assert.Equal(0.7, result.Confidence);
        Assert.Equal(0.3, result.Probabilities[RouteLabel.Billing]);
        Assert.Equal("typesafe", result.Metadata.Provider);
        Assert.Equal("jev-1.13.0", result.Metadata.Model);
        Assert.Equal("request-1", result.Metadata.RequestId);
        Assert.Equal("jev-1.13.0", client.ObservedModel);
        Assert.Equal("site is down", client.ObservedState!["Message"]!.GetValue<string>());
    }

    [Fact]
    public async Task ClassifyAsync_UnknownChoiceThrows()
    {
        var client = new FakeJevClient(Response(
            new JevChoiceAnswer("unknown", 1, new Dictionary<string, double>
            {
                ["billing"] = 0,
                ["technical"] = 0,
                ["unknown"] = 1,
            })));

        var classifier = CreateClassifier(client);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await classifier.ClassifyAsync(new Ticket("x")));

        Assert.Contains("unknown choice", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Build_DuplicateWireNameThrows()
    {
        var builder = new JevChoiceClassifierOptionsBuilder<Ticket, RouteLabel>()
            .WithState(ticket => ticket.Message)
            .WithInstructions("Which team?")
            .AddChoice(RouteLabel.Billing, "same")
            .AddChoice(RouteLabel.Technical, "same");

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    private static JevChoiceClassifier<Ticket, RouteLabel> CreateClassifier(IJevClient client)
    {
        var options = new JevChoiceClassifierOptionsBuilder<Ticket, RouteLabel>()
            .WithState(ticket => ticket.Message)
            .WithInstructions("Which team?")
            .AddChoice(RouteLabel.Billing, "billing")
            .AddChoice(RouteLabel.Technical, "technical")
            .Build();

        return new JevChoiceClassifier<Ticket, RouteLabel>(client, options);
    }

    private static JevSystemOneResponse Response(JevChoiceAnswer answer) =>
        new(
            "jev-1.13.0",
            new Dictionary<string, JevAnswer> { ["route"] = answer },
            new JevUsage(10, 2))
        {
            RequestId = "request-1",
        };

    private sealed class FakeJevClient(JevSystemOneResponse response) : IJevClient
    {
        public JsonNode? ObservedState { get; private set; }
        public string? ObservedModel { get; private set; }
        public string DefaultModel => "jev-latest";

        public ValueTask<JevSystemOneResponse> EvaluateAsync(
            JsonNode? state,
            IReadOnlyDictionary<string, JevQuestion> questions,
            string? model = null,
            CancellationToken cancellationToken = default)
        {
            ObservedState = state;
            ObservedModel = model;
            return ValueTask.FromResult(response);
        }
    }

    private sealed record Ticket(string Message);

    private enum RouteLabel
    {
        Billing,
        Technical,
    }
}
