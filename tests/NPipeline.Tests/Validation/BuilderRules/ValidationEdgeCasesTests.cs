using System.Reflection;
using AwesomeAssertions;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class ValidationEdgeCasesTests
{
    #region Empty Graph Validation Tests

    [Fact]
    public void PipelineBuilder_EmptyGraph_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*NP0101*");
    }

    [Fact]
    public void PipelineBuilder_TryBuildEmptyGraph_ReturnsFailureWithValidationResult()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var success = builder.TryBuild(out var pipeline, out var validationResult);

        // Assert
        success.Should().BeFalse();
        pipeline.Should().BeNull();
        validationResult.Should().NotBeNull();
        validationResult.IsValid.Should().BeFalse();
        validationResult.Errors.Should().Contain("A pipeline must have at least one node.");
    }

    #endregion

    #region Circular Dependency Detection Tests

    [Fact]
    public void PipelineBuilder_SimpleCircularDependency_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        builder.Connect(node1, node2);
        builder.Connect(node2, node1); // Circular

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Any(error => error.Contains("Cycle detected") &&
                                                     (error.Contains("n1 -> n2 -> n1") || error.Contains("n2 -> n1 -> n2"))),
                "cycle path should be included in the error message");
    }

    [Fact]
    public void PipelineBuilder_ComplexCircularDependency_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        var node3 = builder.AddPassThroughTransform<int, int>("n3");
        var sink = builder.AddInMemorySink<int>("sink");

        builder.Connect(source, node1);
        builder.Connect(node1, node2);
        builder.Connect(node2, node3);
        builder.Connect(node3, node1); // Creates cycle: n1 -> n2 -> n3 -> n1
        builder.Connect(node2, sink); // Additional connection to sink

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Any(error => error.Contains("Cycle detected")),
                "should detect cycle in complex graph");
    }

    [Fact]
    public void PipelineBuilder_SelfLoop_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder().WithExtendedValidation();
        var node = builder.AddPassThroughTransform<int, int>("n1");

        // Manually add self-loop edge through the connection state
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(builder)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(node.Id, node.Id));

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Any(error => error.Contains("Self-loop")),
                "should detect self-loop");
    }

    #endregion

    #region Type Mismatch Detection Tests

    [Fact]
    public void PipelineBuilder_TypeMismatchBetweenNodes_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder().WithExtendedValidation();
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var intToString = builder.AddPassThroughTransform<int, string>("intToString");
        var stringSink = builder.AddInMemorySink<string>("stringSink");
        var intSink = builder.AddInMemorySink<int>("intSink");

        builder.Connect(source, intToString);
        builder.Connect(intToString, stringSink); // Valid connection

        // Manually add invalid edge: string -> int
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(builder)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(intToString.Id, intSink.Id));

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Any(error => error.Contains("Type mismatch") &&
                                                     error.Contains("string") &&
                                                     error.Contains("int")),
                "should detect type mismatch between string output and int input");
    }

    [Fact]
    public void PipelineBuilder_GenericTypeMismatch_ThrowsValidationException()
    {
        // Arrange
        var builder = new PipelineBuilder().WithExtendedValidation();
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", ["a", "b", "c"]);
        var stringToInt = builder.AddPassThroughTransform<string, int>("stringToInt");
        var doubleTransform = builder.AddPassThroughTransform<double, string>("doubleTransform");
        var sink = builder.AddInMemorySink<string>("sink");

        builder.Connect(source, stringToInt);

        // Manually add invalid edge: int output -> double input
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(builder)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(stringToInt.Id, doubleTransform.Id));

        builder.Connect(doubleTransform, sink);

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Any(error => error.Contains("Type mismatch") &&
                                                     error.Contains("Int32") &&
                                                     error.Contains("Double")),
                "should detect type mismatch between int output and double input");
    }

    #endregion

    #region Multiple Validation Rule Interaction Tests

    [Fact]
    public void PipelineBuilder_MultipleValidationRules_AllErrorsReported()
    {
        // Arrange
        var builder = new PipelineBuilder().WithExtendedValidation();
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        var node3 = builder.AddPassThroughTransform<int, int>("n3"); // Will be unreachable

        // Create multiple issues:
        // 1. Self-loop on node1
        var connectionState = typeof(PipelineBuilder).GetProperty("ConnectionState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var state = connectionState.GetValue(builder)!;
        var edgesProperty = state.GetType().GetProperty("Edges");
        var edges = (List<Edge>)edgesProperty!.GetValue(state)!;
        edges.Add(new Edge(node1.Id, node1.Id));

        // 2. Connect source to node1 and node2, but leave node3 unreachable
        builder.Connect(source, node1);
        builder.Connect(source, node2);

        // node3 is not connected (unreachable)

        // Act
        var success = builder.TryBuild(out var pipeline, out var validationResult);

        // Assert
        success.Should().BeFalse("build should fail due to validation errors");
        pipeline.Should().BeNull("pipeline should not be created when validation fails");
        validationResult.Errors.Should().Contain(error => error.Contains("Self-loop"));
        validationResult.Errors.Should().Contain(error => error.Contains("Unreachable"));
    }

    [Fact]
    public void PipelineBuilder_CircularDependencyAndUnreachableNodes_ReportsBothErrors()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        var node3 = builder.AddPassThroughTransform<int, int>("n3"); // Unreachable
        var sink = builder.AddInMemorySink<int>("sink");

        // Create circular dependency
        builder.Connect(source, node1);
        builder.Connect(node1, node2);
        builder.Connect(node2, node1);
        builder.Connect(node2, sink);

        // node3 is not connected (unreachable)

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Result.Errors.Count >= 2,
                "should report both cycle and unreachable node errors");

        var exception = act.Should().Throw<PipelineValidationException>().Subject.Single();
        exception.Result.Errors.Should().Contain(error => error.Contains("Cycle detected"));
        exception.Result.Errors.Should().Contain(error => error.Contains("Unreachable"));
    }

    [Fact]
    public void PipelineBuilder_WarnMode_ReportsErrorsButBuildsSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder().WithExtendedValidation().WithValidationMode(GraphValidationMode.Warn);
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        var sink = builder.AddInMemorySink<int>("sink");

        // Create circular dependency
        builder.Connect(source, node1);
        builder.Connect(node1, node2);
        builder.Connect(node2, node1);
        builder.Connect(node2, sink);

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should().NotThrow("pipeline should build successfully in warn mode");

        var pipeline = act();
        pipeline.Should().NotBeNull("pipeline should be created despite validation errors");
    }

    [Fact]
    public void PipelineBuilder_OffMode_SkipsValidationAndBuildsSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder().WithValidationMode(GraphValidationMode.Off);
        var source = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "source", [1, 2, 3]);
        var node1 = builder.AddPassThroughTransform<int, int>("n1");
        var node2 = builder.AddPassThroughTransform<int, int>("n2");
        var sink = builder.AddInMemorySink<int>("sink");

        // Create circular dependency
        builder.Connect(source, node1);
        builder.Connect(node1, node2);
        builder.Connect(node2, node1);
        builder.Connect(node2, sink);

        // Act
        var act = () => builder.Build();

        // Assert
        act.Should().NotThrow("pipeline should build successfully with validation off");

        var pipeline = act();
        pipeline.Should().NotBeNull("pipeline should be created without validation");
    }

    #endregion
}
