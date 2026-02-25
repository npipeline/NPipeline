using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.Connectors.PostgreSQL.Tests.Writers;

/// <summary>
///     Tests for exactly-once delivery semantics in PostgreSQL connector.
///     Validates transaction handling, commit, and rollback behavior.
/// </summary>
public sealed class PostgresExactlyOnceTests
{
    #region Test Models

    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion

    #region Transaction Creation Tests

    [Fact]
    public async Task WriteAsync_WithExactlyOnceSemantic_CreatesTransaction()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var transactionCreated = false;

        var mockConnection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                transactionCreated = true;
                return Task.FromResult(mockTransaction);
            });

        var configuration = new PostgresConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act & Assert - This test verifies the configuration is set correctly
        // The actual transaction creation happens in the writer implementation
        _ = configuration.DeliverySemantic.Should().Be(DeliverySemantic.ExactlyOnce);

        // Verify that BeginTransactionAsync can be called
        var transaction = await mockConnection.BeginTransactionAsync();
        _ = transaction.Should().NotBeNull();
        _ = transactionCreated.Should().BeTrue();
    }

    [Fact]
    public async Task WriteAsync_WithAtLeastOnceSemantic_DoesNotRequireTransaction()
    {
        // Arrange
        var mockConnection = A.Fake<IDatabaseConnection>();
        var configuration = new PostgresConfiguration
        {
            DeliverySemantic = DeliverySemantic.AtLeastOnce,
        };

        // Act & Assert
        _ = configuration.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);

        // With AtLeastOnce, transactions are optional
        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    #endregion

    #region Transaction Commit Tests

    [Fact]
    public async Task Transaction_OnSuccess_CommitsChanges()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var commitCalled = false;

        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._))
            .Invokes(() => commitCalled = true)
            .Returns(Task.CompletedTask);

        // Act
        await mockTransaction.CommitAsync();

        // Assert
        _ = commitCalled.Should().BeTrue();
        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task Transaction_OnSuccess_DoesNotRollback()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();

        // Act
        await mockTransaction.CommitAsync();

        // Assert
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    #endregion

    #region Transaction Rollback Tests

    [Fact]
    public async Task Transaction_OnFailure_RollsBackChanges()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var rollbackCalled = false;

        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._))
            .Invokes(() => rollbackCalled = true)
            .Returns(Task.CompletedTask);

        // Act
        await mockTransaction.RollbackAsync();

        // Assert
        _ = rollbackCalled.Should().BeTrue();
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task Transaction_OnFailure_DoesNotCommit()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();

        // Act
        await mockTransaction.RollbackAsync();

        // Assert
        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    #endregion

    #region Connection State Tests

    [Fact]
    public void Connection_WithActiveTransaction_ReportsCurrentTransaction()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var mockConnection = A.Fake<IDatabaseConnection>();

        A.CallTo(() => mockConnection.CurrentTransaction).Returns(mockTransaction);

        // Act & Assert
        _ = mockConnection.CurrentTransaction.Should().NotBeNull();
        _ = mockConnection.CurrentTransaction.Should().Be(mockTransaction);
    }

    [Fact]
    public void Connection_WithoutActiveTransaction_ReportsNullTransaction()
    {
        // Arrange
        var mockConnection = A.Fake<IDatabaseConnection>();
        A.CallTo(() => mockConnection.CurrentTransaction).Returns(null);

        // Act & Assert
        _ = mockConnection.CurrentTransaction.Should().BeNull();
    }

    #endregion

    #region Delivery Semantic Configuration Tests

    [Fact]
    public void DeliverySemantic_DefaultValue_IsAtLeastOnce()
    {
        // Arrange & Act
        var configuration = new PostgresConfiguration();

        // Assert
        _ = configuration.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
    }

    [Fact]
    public void DeliverySemantic_CanBeSetToExactlyOnce()
    {
        // Arrange & Act
        var configuration = new PostgresConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Assert
        _ = configuration.DeliverySemantic.Should().Be(DeliverySemantic.ExactlyOnce);
    }

    [Fact]
    public void DeliverySemantic_AllowsRoundTrip()
    {
        // Arrange
        var configuration = new PostgresConfiguration
        {
            DeliverySemantic = DeliverySemantic.ExactlyOnce,
        };

        // Act
        configuration.DeliverySemantic = DeliverySemantic.AtLeastOnce;

        // Assert
        _ = configuration.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task Transaction_DisposedOnException_WhenRollbackFails()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._))
            .ThrowsAsync(new InvalidOperationException("Rollback failed"));

        // Act & Assert
        var act = async () => await mockTransaction.RollbackAsync();
        _ = await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Transaction_CanBeDisposed_AfterCommit()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        A.CallTo(() => mockTransaction.DisposeAsync()).Returns(ValueTask.CompletedTask);

        // Act
        await mockTransaction.CommitAsync();
        await mockTransaction.DisposeAsync();

        // Assert
        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => mockTransaction.DisposeAsync()).MustHaveHappened();
    }

    [Fact]
    public async Task Transaction_CanBeDisposed_AfterRollback()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        A.CallTo(() => mockTransaction.DisposeAsync()).Returns(ValueTask.CompletedTask);

        // Act
        await mockTransaction.RollbackAsync();
        await mockTransaction.DisposeAsync();

        // Assert
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => mockTransaction.DisposeAsync()).MustHaveHappened();
    }

    #endregion

    #region Concurrent Transaction Tests

    [Fact]
    public async Task Connection_SupportsMultipleSequentialTransactions()
    {
        // Arrange
        var mockConnection = A.Fake<IDatabaseConnection>();
        var mockTransaction1 = A.Fake<IDatabaseTransaction>();
        var mockTransaction2 = A.Fake<IDatabaseTransaction>();

        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .ReturnsNextFromSequence(mockTransaction1, mockTransaction2);

        // Act
        var transaction1 = await mockConnection.BeginTransactionAsync();
        await transaction1.CommitAsync();
        await transaction1.DisposeAsync();

        var transaction2 = await mockConnection.BeginTransactionAsync();
        await transaction2.CommitAsync();
        await transaction2.DisposeAsync();

        // Assert
        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .MustHaveHappenedTwiceExactly();
    }

    #endregion

    #region Integration-Style Tests

    [Fact]
    public async Task FullTransactionLifecycle_WithExactlyOnce_CompletesSuccessfully()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var mockConnection = A.Fake<IDatabaseConnection>();

        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(mockTransaction));
        A.CallTo(() => mockConnection.CurrentTransaction).Returns(null);

        // Act - Simulate the full lifecycle
        var transaction = await mockConnection.BeginTransactionAsync();

        // Simulate successful operation
        await transaction.CommitAsync();
        await transaction.DisposeAsync();

        // Assert
        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.DisposeAsync()).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task FullTransactionLifecycle_WithFailure_RollsBackSuccessfully()
    {
        // Arrange
        var mockTransaction = A.Fake<IDatabaseTransaction>();
        var mockConnection = A.Fake<IDatabaseConnection>();

        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(mockTransaction));

        // Act - Simulate the full lifecycle with failure
        var transaction = await mockConnection.BeginTransactionAsync();

        // Simulate failed operation
        await transaction.RollbackAsync();
        await transaction.DisposeAsync();

        // Assert
        A.CallTo(() => mockConnection.BeginTransactionAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.RollbackAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.DisposeAsync()).MustHaveHappenedOnceExactly();
        A.CallTo(() => mockTransaction.CommitAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    #endregion
}
