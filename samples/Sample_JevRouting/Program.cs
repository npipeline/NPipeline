using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Decisions.Jev;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_JevRouting;

public sealed record Ticket(string Subject, string Message, string CustomerTier);

public enum TicketRoute
{
    Billing,
    Technical,
    Other,
}

public sealed class TicketSource : SourceNode<Ticket>
{
    public override IDataStream<Ticket> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        Ticket[] tickets =
        [
            new("Duplicate charge", "My card was charged twice for one invoice.", "standard"),
            new("API unavailable", "Every API request returns HTTP 503.", "enterprise"),
            new("Partnership request", "We would like to discuss a joint webinar.", "standard"),
        ];

        return new InMemoryDataStream<Ticket>(tickets, "tickets");
    }
}

public abstract class TicketSink(string queue) : SinkNode<Ticket>
{
    public override async Task ConsumeAsync(
        IDataStream<Ticket> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await foreach (var ticket in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"[{queue}] {ticket.Subject}");
        }
    }
}

public sealed class BillingSink() : TicketSink("billing");

public sealed class TechnicalSink() : TicketSink("technical");

public sealed class ReviewSink() : TicketSink("review");

public sealed class JevRoutingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var jevClient = (IJevClient)context.Items["jevClient"];
        var source = builder.AddSource<TicketSource, Ticket>("tickets");
        var billing = builder.AddSink<BillingSink, Ticket>("billing");
        var technical = builder.AddSink<TechnicalSink, Ticket>("technical");
        var review = builder.AddSink<ReviewSink, Ticket>("review");

        var route = builder.AddJevRoute<Ticket, TicketRoute>(jevClient, options => options
                    .WithState(ticket => new
                    {
                        ticket.Subject,
                        ticket.Message,
                        ticket.CustomerTier,
                    })
                    .WithInstructions("Which team should handle this ticket?")
                    .AddChoice(TicketRoute.Billing, "billing", "Charges, invoices, and refunds")
                    .AddChoice(TicketRoute.Technical, "technical", "Bugs, outages, and integrations")
                    .AddChoice(TicketRoute.Other, "other", "None of the other routes apply"),
                "ticket-route")
            .WhenLabel(TicketRoute.Billing, billing, 0.75)
            .WhenLabel(TicketRoute.Technical, technical, 0.70)
            .Otherwise(review);

        _ = builder.Connect(source, route);
    }
}

public static class Program
{
    public static async Task Main()
    {
        var apiKey = Environment.GetEnvironmentVariable("TYPESAFE_API_KEY");

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            Console.Error.WriteLine("Set TYPESAFE_API_KEY before running this sample.");
            Environment.ExitCode = 1;
            return;
        }

        using var httpClient = new HttpClient();
        var context = PipelineContext.CreateDefault();
        context.Items["jevClient"] = new JevClient(httpClient, new JevClientOptions { ApiKey = apiKey });

        await PipelineRunner.Create().RunAsync<JevRoutingPipeline>(context);
    }
}
