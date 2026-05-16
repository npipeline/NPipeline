using System.Collections.Frozen;
using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Services;

public sealed class DataStreamWrapperServiceRouteOptionsTests
{
    [Fact]
    public async Task WrapWithCountingAndBranching_LineageWrappedStream_ThrowsWithoutBindNormalization()
    {
        const string routeNodeId = "route";

        var source = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>(
        [
            CreatePacket(1),
            CreatePacket(2),
            CreatePacket(3),
            CreatePacket(4),
        ],
            "Rewrapped_DefaultStream");

        var routeOptions = new RouteOptions<int>()
            .When("even", value => value % 2 == 0)
            .Otherwise("odd");

        var evenEdge = new Edge(routeNodeId, "even-sink", "even");
        var oddEdge = new Edge(routeNodeId, "odd-sink", "odd");

        var graph = CreateRouteGraph(routeNodeId, routeOptions, false, evenEdge, oddEdge);
        var counter = new StatsCounter();
        var service = new DataStreamWrapperService();

        Action act = () =>
        {
            _ = service.WrapWithCountingAndBranching(
                source,
                counter,
                PipelineContext.Default,
                graph,
                routeNodeId);
        };

        var thrown = act.Should().Throw<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain("Route options type mismatch");
        _ = thrown.Which.Message.Should().Contain("RuntimePipelineBinder");
    }

    [Fact]
    public async Task WrapWithCountingAndBranching_LineageWrappedStream_UsesBindNormalizedRouteOptions()
    {
        const string routeNodeId = "route";

        var source = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>(
        [
            CreatePacket(1),
            CreatePacket(2),
            CreatePacket(3),
            CreatePacket(4),
        ],
            "Rewrapped_DefaultStream");

        var routeOptions = new RouteOptions<int>()
            .When("even", value => value % 2 == 0)
            .Otherwise("odd");

        var evenEdge = new Edge(routeNodeId, "even-sink", "even");
        var oddEdge = new Edge(routeNodeId, "odd-sink", "odd");

        var graph = CreateRouteGraph(routeNodeId, routeOptions, true, evenEdge, oddEdge);
        var bound = await RuntimePipelineBinder.Instance.BindAsync(graph, PipelineContext.Default);

        var counter = new StatsCounter();
        var service = new DataStreamWrapperService();

        await using var wrapped = service.WrapWithCountingAndBranching(
            source,
            counter,
            PipelineContext.Default,
            bound.Graph,
            routeNodeId);

        var edgeRouted = wrapped.Should().BeAssignableTo<IEdgeRoutedDataStream>().Subject;

        await using var evenView = edgeRouted.GetEdgeView(evenEdge);
        await using var oddView = edgeRouted.GetEdgeView(oddEdge);

        var evenValuesTask = ReadPayloadsAsync((IDataStream<LineagePacket<int>>)evenView);
        var oddValuesTask = ReadPayloadsAsync((IDataStream<LineagePacket<int>>)oddView);

        await Task.WhenAll(evenValuesTask, oddValuesTask);

        var evenValues = await evenValuesTask;
        var oddValues = await oddValuesTask;

        _ = evenValues.Should().BeEquivalentTo([2, 4]);
        _ = oddValues.Should().BeEquivalentTo([1, 3]);
        _ = counter.Total.Should().Be(4);
    }

    private static PipelineGraph CreateRouteGraph<TPayload>(
        string nodeId,
        RouteOptions<TPayload> routeOptions,
        bool itemLevelLineageEnabled,
        params Edge[] edges)
    {
        var routeNode = new NodeDefinition(
            nodeId,
            nodeId,
            typeof(object),
            NodeKind.Route,
            typeof(TPayload),
            typeof(TPayload));

        return new PipelineGraph
        {
            Nodes = [routeNode],
            Edges = [.. edges],
            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
            NodeDefinitionMap = new Dictionary<string, NodeDefinition> { [nodeId] = routeNode }.ToFrozenDictionary(),
            ExecutionOptions = new ExecutionOptionsConfiguration
            {
                NodeExecutionAnnotations = ImmutableDictionary<string, object>.Empty
                    .Add(ExecutionAnnotationKeys.RouteOptionsForNode(nodeId), routeOptions),
            },
            Lineage = new LineageConfiguration
            {
                ItemLevelLineageEnabled = itemLevelLineageEnabled,
            },
        };
    }

    private static async Task<List<int>> ReadPayloadsAsync(IDataStream<LineagePacket<int>> stream)
    {
        List<int> values = [];

        await foreach (var packet in stream)
        {
            values.Add(packet.Data);
        }

        return values;
    }

    private static LineagePacket<int> CreatePacket(int value)
    {
        return new LineagePacket<int>(value, Guid.NewGuid(), ImmutableList<string>.Empty);
    }
}
