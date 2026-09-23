using System.Data;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Tests.Reliability.Behavior;

/// <summary>
///     An <see cref="IDatabaseConnection" /> that records every command it executes and fails the ones a script picks, so a
///     test can assert what reached the server.
/// </summary>
internal sealed class ScriptedConnection : IDatabaseConnection
{
    private readonly Func<int, ExecutedCommand, Exception?> _failure;
    private readonly Func<ExecutedCommand, IReadOnlyList<IReadOnlyDictionary<string, object?>>> _results;

    /// <param name="failure">Given the zero-based execution index and the command, returns the exception to throw, or null to succeed.</param>
    /// <param name="results">Given a command run as a reader, returns the rows it answers with. Defaults to none.</param>
    public ScriptedConnection(
        Func<int, ExecutedCommand, Exception?>? failure = null,
        Func<ExecutedCommand, IReadOnlyList<IReadOnlyDictionary<string, object?>>>? results = null)
    {
        _failure = failure ?? ((_, _) => null);
        _results = results ?? (_ => []);
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

    private async Task<IDatabaseReader> QueryAsync(ExecutedCommand command, CancellationToken cancellationToken)
    {
        _ = await ExecuteAsync(command, cancellationToken).ConfigureAwait(false);
        return new Reader(_results(command));
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
            return connection.QueryAsync(new ExecutedCommand(CommandText, [.. _parameters]), cancellationToken);
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

    private sealed class Reader(IReadOnlyList<IReadOnlyDictionary<string, object?>> rows) : IDatabaseReader
    {
        private int _index = -1;

        private IReadOnlyDictionary<string, object?> Current => rows[_index];

        public bool HasRows => rows.Count > 0;

        public int FieldCount => Current.Count;

        public string GetName(int ordinal)
        {
            return Current.Keys.ElementAt(ordinal);
        }

        public Type GetFieldType(int ordinal)
        {
            return Current.Values.ElementAt(ordinal)?.GetType() ?? typeof(object);
        }

        public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(++_index < rows.Count);
        }

        public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public T? GetFieldValue<T>(int ordinal)
        {
            return (T?)Current.Values.ElementAt(ordinal);
        }

        public bool IsDBNull(int ordinal)
        {
            return Current.Values.ElementAt(ordinal) is null;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
