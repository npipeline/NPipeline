using System.Text.Json.Nodes;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions.Jev.Tests;

public sealed class JevRouteIntegrationTests
{
    [Fact]
    public async Task AddJevRoute_ClassifiesOnceAndRoutesOriginalItem()
    {
        var ticket = new Ticket("The site is unavailable.");
        var technical = new InMemorySinkNode<Ticket>();
        var review = new InMemorySinkNode<Ticket>();
        var client = new FakeJevClient();
        var context = PipelineContext.CreateDefault();
        context.Items["ticket"] = ticket;
        context.Items["technical"] = technical;
        context.Items["review"] = review;
        context.Items["client"] = client;

        await PipelineRunner.Create().RunAsync<JevRoutePipeline>(context);

        Assert.Same(ticket, Assert.Single(technical.Items));
        Assert.Empty(review.Items);
        Assert.Equal(1, client.CallCount);
        var question = Assert.IsType<JevChoiceQuestion>(client.Questions!["route"]);
        Assert.Equal(3, question.Criteria.Count);
    }

    private sealed class JevRoutePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var ticket = (Ticket)context.Items["ticket"];
            var technicalSink = (InMemorySinkNode<Ticket>)context.Items["technical"];
            var reviewSink = (InMemorySinkNode<Ticket>)context.Items["review"];
            var client = (IJevClient)context.Items["client"];
            var source = builder.AddInMemorySource("source", [ticket]);
            var technical = builder.AddSink<InMemorySinkNode<Ticket>, Ticket>("technical");
            var review = builder.AddSink<InMemorySinkNode<Ticket>, Ticket>("review");
            builder.AddPreconfiguredNodeInstance(technical.Id, technicalSink);
            builder.AddPreconfiguredNodeInstance(review.Id, reviewSink);

            var route = builder.AddJevRoute<Ticket, RouteLabel>(client, options => options
                    .WithState(item => new { item.Message })
                    .WithInstructions("Which team should handle this ticket?")
                    .AddChoice(RouteLabel.Billing, "billing", "Charges and refunds")
                    .AddChoice(RouteLabel.Technical, "technical", "Bugs and outages")
                    .AddChoice(RouteLabel.Other, "other", "None of the other routes apply"))
                .WhenLabel(RouteLabel.Technical, technical, minimumConfidence: 0.75)
                .Otherwise(review);

            builder.Connect(source, route);
        }
    }

    private sealed class FakeJevClient : IJevClient
    {
        public string DefaultModel => "jev-latest";
        public int CallCount { get; private set; }
        public IReadOnlyDictionary<string, JevQuestion>? Questions { get; private set; }

        public ValueTask<JevSystemOneResponse> EvaluateAsync(
            JsonNode? state,
            IReadOnlyDictionary<string, JevQuestion> questions,
            string? model = null,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            Questions = questions;
            var answer = new JevChoiceAnswer(
                "technical",
                0.9,
                new Dictionary<string, double>
                {
                    ["billing"] = 0.05,
                    ["technical"] = 0.9,
                    ["other"] = 0.05,
                });
            return ValueTask.FromResult(new JevSystemOneResponse(
                "jev-1.13.0",
                new Dictionary<string, JevAnswer> { ["route"] = answer },
                new JevUsage(25, 8)));
        }
    }

    private sealed record Ticket(string Message);

    private enum RouteLabel
    {
        Billing,
        Technical,
        Other,
    }
}