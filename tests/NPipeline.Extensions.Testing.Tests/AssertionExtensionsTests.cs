// ReSharper disable ClassNeverInstantiated.Global

using FluentAssertions;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.Testing.AwesomeAssertions;
using NPipeline.Pipeline;
using InMemorySinkExtensions = NPipeline.Extensions.Testing.FluentAssertions.InMemorySinkExtensions;

namespace NPipeline.Extensions.Testing.Tests;

public class AssertionExtensionsTests
{
    // Tests for AwesomeAssertions extensions
    public class AwesomeAssertionsTests
    {
        [Fact]
        public async Task ShouldHaveReceived_WithCorrectCount_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldHaveReceived(3);
        }

        [Fact]
        public async Task ShouldHaveReceived_WithIncorrectCount_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => sink.ShouldHaveReceived(5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task ShouldContain_WithExistingItem_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldContain(2);
        }

        [Fact]
        public async Task ShouldContain_WithNonExistingItem_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => sink.ShouldContain(5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task ShouldContain_WithPredicate_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldContain(x => x > 2);
        }

        [Fact]
        public async Task ShouldContain_WithNonMatchingPredicate_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => sink.ShouldContain(x => x > 5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task ShouldNotContain_WithNonExistingItem_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldNotContain(5);
        }

        [Fact]
        public async Task ShouldNotContain_WithExistingItem_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => sink.ShouldNotContain(2);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task ShouldOnlyContain_WithMatchingPredicate_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([2, 4, 6]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldOnlyContain(x => x % 2 == 0);
        }

        [Fact]
        public async Task ShouldOnlyContain_WithNonMatchingPredicate_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => sink.ShouldOnlyContain(x => x % 2 == 0);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public void ShouldHaveReceived_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => sink!.ShouldHaveReceived(1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void ShouldContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => sink!.ShouldContain(1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void ShouldNotContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => sink!.ShouldNotContain(1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void ShouldOnlyContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => sink!.ShouldOnlyContain(x => true);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public async Task ShouldContain_WithComplexTypes_ShouldWork()
        {
            // Arrange
            var sink = new InMemorySinkNode<TestObject>();
            var context = PipelineContext.Default;

            var data = new InMemoryDataPipe<TestObject>([
                new TestObject { Name = "Test1", Value = 1 },
                new TestObject { Name = "Test2", Value = 2 },
            ]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            sink.ShouldContain(x => x.Name == "Test1");
            sink.ShouldContain(new TestObject { Name = "Test2", Value = 2 });
            sink.ShouldNotContain(new TestObject { Name = "Test3", Value = 3 });
            sink.ShouldOnlyContain(x => x.Value > 0);
        }
    }

    // Tests for FluentAssertions extensions
    public class FluentAssertionsExtensionTests
    {
        [Fact]
        public async Task FluentShouldHaveReceived_WithCorrectCount_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldHaveReceived(sink, 3);
        }

        [Fact]
        public async Task FluentShouldHaveReceived_WithIncorrectCount_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => InMemorySinkExtensions.ShouldHaveReceived(sink, 5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task FluentShouldContain_WithExistingItem_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldContain(sink, 2);
        }

        [Fact]
        public async Task FluentShouldContain_WithNonExistingItem_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => InMemorySinkExtensions.ShouldContain(sink, 5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task FluentShouldContain_WithPredicate_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldContain(sink, x => x > 2);
        }

        [Fact]
        public async Task FluentShouldContain_WithNonMatchingPredicate_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => InMemorySinkExtensions.ShouldContain(sink, x => x > 5);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task FluentShouldNotContain_WithNonExistingItem_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldNotContain(sink, 5);
        }

        [Fact]
        public async Task FluentShouldNotContain_WithExistingItem_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => InMemorySinkExtensions.ShouldNotContain(sink, 2);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public async Task FluentShouldOnlyContain_WithMatchingPredicate_ShouldNotThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([2, 4, 6]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldOnlyContain(sink, x => x % 2 == 0);
        }

        [Fact]
        public async Task FluentShouldOnlyContain_WithNonMatchingPredicate_ShouldThrow()
        {
            // Arrange
            var sink = new InMemorySinkNode<int>();
            var context = PipelineContext.Default;
            var data = new InMemoryDataPipe<int>([1, 2, 3]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            var act = () => InMemorySinkExtensions.ShouldOnlyContain(sink, x => x % 2 == 0);
            act.Should().Throw<Exception>();
        }

        [Fact]
        public void FluentShouldHaveReceived_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => InMemorySinkExtensions.ShouldHaveReceived(sink!, 1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void FluentShouldContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => InMemorySinkExtensions.ShouldContain(sink!, 1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void FluentShouldNotContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => InMemorySinkExtensions.ShouldNotContain(sink!, 1);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public void FluentShouldOnlyContain_WithNullSink_ShouldThrowArgumentNullException()
        {
            // Arrange
            InMemorySinkNode<int>? sink = null;

            // Act & Assert
            var act = () => InMemorySinkExtensions.ShouldOnlyContain(sink!, x => true);
            act.Should().Throw<ArgumentNullException>();
        }

        [Fact]
        public async Task FluentShouldContain_WithComplexTypes_ShouldWork()
        {
            // Arrange
            var sink = new InMemorySinkNode<TestObject>();
            var context = PipelineContext.Default;

            var data = new InMemoryDataPipe<TestObject>([
                new TestObject { Name = "Test1", Value = 1 },
                new TestObject { Name = "Test2", Value = 2 },
            ]);

            // Act
            await sink.ExecuteAsync(data, context, CancellationToken.None);

            // Assert
            InMemorySinkExtensions.ShouldContain(sink, x => x.Name == "Test1");
            InMemorySinkExtensions.ShouldContain(sink, new TestObject { Name = "Test2", Value = 2 });
            InMemorySinkExtensions.ShouldNotContain(sink, new TestObject { Name = "Test3", Value = 3 });
            InMemorySinkExtensions.ShouldOnlyContain(sink, x => x.Value > 0);
        }
    }

    // Helper class for testing complex types
    private sealed class TestObject
    {
        public string? Name { get; set; }
        public int Value { get; set; }

        public override bool Equals(object? obj)
        {
            if (obj is TestObject other)
                return Name == other.Name && Value == other.Value;

            return false;
        }

        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return HashCode.Combine(Name, Value);
        }
    }
}
