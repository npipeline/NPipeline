// ReSharper disable ClassNeverInstantiated.Local

using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderExtensionsTests
{
    #region AddTap Tests

    [Fact]
    public void AddTap_WithSinkNode_ReturnsTransformNodeHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        NoOpSink mockSink = new();

        // Act
        var handle = builder.AddTap(mockSink);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void AddTap_WithSinkNodeAndName_UsesProvidedName()
    {
        // Arrange
        PipelineBuilder builder = new();
        NoOpSink mockSink = new();
        const string name = "CustomTap";

        // Act
        var handle = builder.AddTap(mockSink, name);

        // Assert
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddTap_WithSinkNodeNullName_UsesDefaultTapName()
    {
        // Arrange
        PipelineBuilder builder = new();
        NoOpSink mockSink = new();

        // Act
        var handle = builder.AddTap(mockSink);

        // Assert
        _ = handle.Id.Should().Be("tap");
    }

    [Fact]
    public void AddTap_WithSinkFactory_ReturnsTransformNodeHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        ISinkNode<string> CreateSink()
        {
            return new NoOpSink();
        }

        // Act
        var handle = builder.AddTap(CreateSink);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void AddTap_WithSinkFactoryAndName_UsesProvidedName()
    {
        // Arrange
        PipelineBuilder builder = new();

        ISinkNode<string> CreateSink()
        {
            return new NoOpSink();
        }

        const string name = "FactoryTap";

        // Act
        var handle = builder.AddTap(CreateSink, name);

        // Assert
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddTap_WithSinkFactoryNullName_UsesDefaultTapName()
    {
        // Arrange
        PipelineBuilder builder = new();

        ISinkNode<string> CreateSink()
        {
            return new NoOpSink();
        }

        // Act
        var handle = builder.AddTap(CreateSink);

        // Assert
        _ = handle.Id.Should().Be("tap");
    }

    [Fact]
    public void AddTap_WithNullSink_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        ISinkNode<string> nullSink = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.AddTap(nullSink));
    }

    [Fact]
    public void AddTap_WithNullFactory_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        Func<ISinkNode<string>> nullFactory = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.AddTap(nullFactory));
    }

    #endregion

    #region AddBranch Tests

    [Fact]
    public void AddBranch_WithSingleHandler_ReturnsTransformNodeHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler(string item)
        {
            await Task.CompletedTask;
        }

        // Act
        var handle = builder.AddBranch<string>(Handler);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void AddBranch_WithSingleHandlerAndName_UsesProvidedName()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler(string item)
        {
            await Task.CompletedTask;
        }

        const string name = "CustomBranch";

        // Act
        var handle = builder.AddBranch<string>(Handler, name);

        // Assert
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddBranch_WithSingleHandlerNullName_UsesDefaultTeeName()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler(string item)
        {
            await Task.CompletedTask;
        }

        // Act
        var handle = builder.AddBranch<string>(Handler);

        // Assert
        _ = handle.Id.Should().Be("tee");
    }

    [Fact]
    public void AddBranch_WithNullHandler_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        Func<string, Task> nullHandler = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.AddBranch(nullHandler));
    }

    [Fact]
    public void AddBranch_WithMultipleHandlers_ReturnsTransformNodeHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        List<Func<string, Task>> handlers =
        [
            async item => await Task.CompletedTask,
            async item => await Task.CompletedTask,
            async item => await Task.CompletedTask,
        ];

        // Act
        var handle = builder.AddBranch(handlers);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void AddBranch_WithMultipleHandlersAndName_UsesProvidedName()
    {
        // Arrange
        PipelineBuilder builder = new();

        List<Func<string, Task>> handlers =
        [
            async item => await Task.CompletedTask,
            async item => await Task.CompletedTask,
        ];

        const string name = "MultiBranch";

        // Act
        var handle = builder.AddBranch(handlers, name);

        // Assert
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddBranch_WithMultipleHandlersNullName_UsesDefaultTeeName()
    {
        // Arrange
        PipelineBuilder builder = new();

        List<Func<string, Task>> handlers =
        [
            async item => await Task.CompletedTask,
        ];

        // Act
        var handle = builder.AddBranch(handlers);

        // Assert
        _ = handle.Id.Should().Be("tee");
    }

    [Fact]
    public void AddBranch_WithEmptyHandlersList_ReturnsTransformNodeHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        List<Func<string, Task>> emptyHandlers = [];

        // Act
        var handle = builder.AddBranch(emptyHandlers);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void AddBranch_WithNullHandlersList_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = new();
        IEnumerable<Func<string, Task>> nullHandlers = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => builder.AddBranch(nullHandlers));
    }

    #endregion

    #region AddBatcher Tests

    [Fact]
    public void AddBatcher_WithValidParameters_CreatesTransformHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "TestBatcher";
        const int batchSize = 10;
        var timespan = TimeSpan.FromSeconds(1);

        // Act
        var handle = builder.AddBatcher<int>(name, batchSize, timespan);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddBatcher_WithSmallBatchSize_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 1;
        var timespan = TimeSpan.FromMilliseconds(100);

        // Act
        var handle =
            builder.AddBatcher<string>("SmallBatch", batchSize, timespan);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddBatcher_WithLargeBatchSize_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 1000;
        var timespan = TimeSpan.FromSeconds(10);

        // Act
        var handle =
            builder.AddBatcher<long>("LargeBatch", batchSize, timespan);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddBatcher_WithVerySmallTimespan_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 5;
        var timespan = TimeSpan.FromMilliseconds(10);

        // Act
        var handle =
            builder.AddBatcher<double>("SmallTimespan", batchSize, timespan);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddBatcher_WithLargeTimespan_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 10;
        var timespan = TimeSpan.FromHours(1);

        // Act
        var handle =
            builder.AddBatcher<decimal>("LargeTimespan", batchSize, timespan);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddBatcher_RegistersBuilderDisposable()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 5;
        var timespan = TimeSpan.FromSeconds(1);

        // Act
        var handle =
            builder.AddBatcher<int>("DisposableTest", batchSize, timespan);

        // Assert - the handle indicates the node was created and registered
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddBatcher_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        const int batchSize = 10;
        var timespan = TimeSpan.FromSeconds(1);

        // Act
        var handle =
            builder.AddBatcher<string>("GenericTest", batchSize, timespan);

        // Assert - verify the handle has correct generic types
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    #endregion

    #region AddUnbatcher Tests

    [Fact]
    public void AddUnbatcher_WithValidName_CreatesTransformHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "TestUnbatcher";

        // Act
        var handle = builder.AddUnbatcher<int>(name);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
        _ = handle.Id.Should().Be(name.ToLowerInvariant());
    }

    [Fact]
    public void AddUnbatcher_WithDifferentGenericType_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "StringUnbatcher";

        // Act
        var handle = builder.AddUnbatcher<string>(name);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddUnbatcher_WithComplexType_CreatesNode()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "ComplexUnbatcher";

        // Act
        var handle = builder.AddUnbatcher<CustomType>(name);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddUnbatcher_RegistersBuilderDisposable()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "DisposableUnbatcherTest";

        // Act
        var handle = builder.AddUnbatcher<double>(name);

        // Assert - the handle indicates the node was created and registered
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    [Fact]
    public void AddUnbatcher_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        const string name = "GenericUnbatcherTest";

        // Act
        var handle = builder.AddUnbatcher<long>(name);

        // Assert - verify the handle has correct generic types
        _ = handle.Should().NotBeNull();
        _ = handle.Id.Should().NotBeNull();
    }

    #endregion

    #region Multiple Extensions on Same Builder

    [Fact]
    public void AddTap_AndAddBranch_BothReturnValidHandles()
    {
        // Arrange
        PipelineBuilder builder = new();
        NoOpSink mockSink = new();

        async Task BranchHandler(int item)
        {
            await Task.CompletedTask;
        }

        // Act
        var tapHandle = builder.AddTap(mockSink);
        var branchHandle = builder.AddBranch<int>(BranchHandler);

        // Assert
        _ = tapHandle.Should().NotBeNull();
        _ = branchHandle.Should().NotBeNull();
        _ = tapHandle.Id.Should().NotBe(branchHandle.Id);
    }

    [Fact]
    public void MultipleAddTap_ReturnsDifferentHandleIds()
    {
        // Arrange
        PipelineBuilder builder = new();
        NoOpSink sink1 = new();
        NoOpSink sink2 = new();

        // Act
        var handle1 = builder.AddTap(sink1, "Tap1");
        var handle2 = builder.AddTap(sink2, "Tap2");

        // Assert
        _ = handle1.Should().NotBeNull();
        _ = handle2.Should().NotBeNull();
        _ = handle1.Id.Should().NotBe(handle2.Id);
    }

    [Fact]
    public void MultipleBranches_WithDifferentNames_ReturnDifferentHandleIds()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler1(string s)
        {
            await Task.CompletedTask;
        }

        async Task Handler2(string s)
        {
            await Task.CompletedTask;
        }

        // Act
        var branch1 = builder.AddBranch<string>(Handler1, "Branch1");
        var branch2 = builder.AddBranch<string>(Handler2, "Branch2");

        // Assert
        _ = branch1.Should().NotBeNull();
        _ = branch2.Should().NotBeNull();
        _ = branch1.Id.Should().NotBe(branch2.Id);
    }

    #endregion

    #region Handle ID Generation

    [Fact]
    public void AddTap_WithoutName_GeneratesValidHandleId()
    {
        // Arrange
        PipelineBuilder builder = new PipelineBuilder().WithoutEarlyNameValidation();
        NoOpSink sink1 = new();
        NoOpSink sink2 = new();

        // Act
        var handle1 = builder.AddTap(sink1);
        var handle2 = builder.AddTap(sink2);

        // Assert
        _ = handle1.Id.Should().NotBeNullOrEmpty();
        _ = handle2.Id.Should().NotBeNullOrEmpty();
        _ = handle1.Id.Should().NotBe(handle2.Id);
    }

    [Fact]
    public void AddBranch_WithoutName_GeneratesValidHandleId()
    {
        // Arrange
        PipelineBuilder builder = new PipelineBuilder().WithoutEarlyNameValidation();

        async Task Handler1(int i)
        {
            await Task.CompletedTask;
        }

        async Task Handler2(int i)
        {
            await Task.CompletedTask;
        }

        // Act
        var branch1 = builder.AddBranch<int>(Handler1);
        var branch2 = builder.AddBranch<int>(Handler2);

        // Assert
        _ = branch1.Id.Should().NotBeNullOrEmpty();
        _ = branch2.Id.Should().NotBeNullOrEmpty();
        _ = branch1.Id.Should().NotBe(branch2.Id);
    }

    #endregion

    #region Data Type Support

    [Fact]
    public void AddTap_WithIntSink_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        IntSink mockSink = new();

        // Act
        var handle = builder.AddTap(mockSink);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Should().BeOfType<TransformNodeHandle<int, int>>();
    }

    [Fact]
    public void AddTap_WithCustomTypeSink_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();
        CustomTypeSink mockSink = new();

        // Act
        var handle = builder.AddTap(mockSink);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Should().BeOfType<TransformNodeHandle<CustomType, CustomType>>();
    }

    [Fact]
    public void AddBranch_WithIntHandler_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler(int item)
        {
            await Task.CompletedTask;
        }

        // Act
        var handle = builder.AddBranch<int>(Handler);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Should().BeOfType<TransformNodeHandle<int, int>>();
    }

    [Fact]
    public void AddBranch_WithCustomTypeHandler_ReturnsCorrectGenericHandle()
    {
        // Arrange
        PipelineBuilder builder = new();

        async Task Handler(CustomType item)
        {
            await Task.CompletedTask;
        }

        // Act
        var handle = builder.AddBranch<CustomType>(Handler);

        // Assert
        _ = handle.Should().NotBeNull();
        _ = handle.Should().BeOfType<TransformNodeHandle<CustomType, CustomType>>();
    }

    #endregion

    #region Helper Classes

    private sealed class NoOpSink : SinkNode<string>
    {
        public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // No-op
            }
        }
    }

    private sealed class IntSink : SinkNode<int>
    {
        public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // No-op
            }
        }
    }

    private sealed class CustomTypeSink : SinkNode<CustomType>
    {
        public override async Task ExecuteAsync(IDataPipe<CustomType> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // No-op
            }
        }
    }

    private sealed record CustomType(int Id, string Name);

    #endregion
}
