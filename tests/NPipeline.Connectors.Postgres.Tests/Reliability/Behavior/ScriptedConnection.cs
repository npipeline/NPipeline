using System.Data;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Postgres.Tests.Reliability.Behavior;

/// <summary>
///     An <see cref="IDatabaseConnection" /> that records every command it executes and fails the ones a script picks, so a
///     test can assert what reached the server.
/// </summary>
internal sealed class ScriptedConnection : IDatabaseConnection
{
    private readonly Func<int, ExecutedCommand, Exception?> _failure;

    /// <param name="failure">Given the zero-based execution index and the command, returns the exception to throw, or null to succeed.</param>
    public ScriptedConnection(Func<int, ExecutedCommand, Exception?>? failure = null)
    {
        _failure = failure ?? ((_, _) => null);
    }

    /// <summary>Every command executed, including the ones that failed.</summary>
    public List<ExecutedCommand> Executed { get; } = [];

    /// <summary>The commands that succeeded, which is what the server kept.</summary>
    public List<ExecutedCommand> Committed { get; } = [];

    public int Opens { get; private set; }

    public bool IsOpen { get; set; } = true;

    public IDatabaseTransaction? CurrentTransaction { get; set; }

    public Task OpenAsync(CancellationToken cancellationToken = default)
    {
        Opens++;
        IsOpen = true;
        return Task.CompletedTask;
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        IsOpen = false;
        return Task.CompletedTask;
    }

    public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IDatabaseCommand>(new Command(this));
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    private Task<int> ExecuteAsync(ExecutedCommand command, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var index = Executed.Count;
        Executed.Add(command);

        if (_failure(index, command) is { } failure)
            throw failure;

        Committed.Add(command);
        return Task.FromResult(1);
    }

    internal sealed record ExecutedCommand(string Text, IReadOnlyList<object?> Parameters);

    private sealed class Command(ScriptedConnection connection) : IDatabaseCommand
    {
        private readonly List<object?> _parameters = [];

        public string CommandText { get; set; } = string.Empty;

        public int CommandTimeout { get; set; }

        public CommandType CommandType { get; set; }

        public void AddParameter(string name, object? value)
        {
            _parameters.Add(value);
        }

        public Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        {
            return connection.ExecuteAsync(new ExecutedCommand(CommandText, [.. _parameters]), cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
