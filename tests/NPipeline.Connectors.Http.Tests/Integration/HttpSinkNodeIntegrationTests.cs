using System.Text.Json;
using NPipeline.Connectors.Http.Configuration;
using NPipeline.Connectors.Http.Nodes;
using NPipeline.Connectors.Http.Tests.Fixtures;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace NPipeline.Connectors.Http.Tests.Integration;

[Collection("Http")]
public class HttpSinkNodeIntegrationTests(WireMockFixture fixture)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static StreamingDataPipe<T> PipeOf<T>(params T[] items)
    {
        async IAsyncEnumerable<T> Generate()
        {
            foreach (var item in items)
            {
                yield return item;
            }

            await Task.CompletedTask;
        }

        return new StreamingDataPipe<T>(Generate(), "test");
    }

    [Fact]
    public async Task HttpSinkNode_SingleItemPost_SendsJsonPayload()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/orders").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(201));

        using var httpClient = new HttpClient();
        var config = new HttpSinkConfiguration { Uri = new Uri($"{fixture.BaseUrl}/orders") };
        var node = new HttpSinkNode<Order>(config, httpClient);

        await using var pipe = PipeOf(new Order(1, "Widget", 9.99m));
        await node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        var logEntries = fixture.Server.LogEntries;
        logEntries.Should().HaveCount(1);
        var requestBody = logEntries.Single().RequestMessage.Body!;

        var order = JsonSerializer.Deserialize<Order>(requestBody, JsonOptions);

        order!.Product.Should().Be("Widget");
    }

    [Fact]
    public async Task HttpSinkNode_BatchedItems_SendsMultipleRequests()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/orders").UsingPost())
            .RespondWith(Response.Create().WithStatusCode(200));

        using var httpClient = new HttpClient();

        var config = new HttpSinkConfiguration
        {
            Uri = new Uri($"{fixture.BaseUrl}/orders"),
            BatchSize = 2,
        };

        var node = new HttpSinkNode<Order>(config, httpClient);

        await using var pipe = PipeOf(
            new Order(1, "A", 1m),
            new Order(2, "B", 2m),
            new Order(3, "C", 3m)); // → 2 requests: [1,2] and [3]

        await node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        fixture.Server.LogEntries.Should().HaveCount(2);
    }

    [Fact]
    public async Task HttpSinkNode_PutMethod_UsesCorrectMethod()
    {
        fixture.Server.Reset();

        fixture.Server
            .Given(Request.Create().WithPath("/orders/1").UsingPut())
            .RespondWith(Response.Create().WithStatusCode(200));

        using var httpClient = new HttpClient();

        var config = new HttpSinkConfiguration
        {
            UriFactory = item => new Uri($"{fixture.BaseUrl}/orders/{((Order)item).Id}"),
            Method = SinkHttpMethod.Put,
        };

        var node = new HttpSinkNode<Order>(config, httpClient);

        await using var pipe = PipeOf(new Order(1, "Widget", 9.99m));
        await node.ExecuteAsync(pipe, new PipelineContext(), CancellationToken.None);

        fixture.Server.LogEntries.Should().HaveCount(1);
    }

    private sealed record Order(int Id, string Product, decimal Price);
}
