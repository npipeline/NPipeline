using NPipeline.Connectors.Configuration;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Checkpointing.Strategies;

/// <summary>
///     Handler for offset-based checkpointing.
///     Tracks numeric offsets (e.g., sequential ID tracking).
/// </summary>
public class OffsetCheckpointHandler
{
    private readonly CheckpointManager _checkpointManager;
    private readonly string _offsetColumn;
    private readonly string _quotedOffsetColumn;

    /// <summary>
    ///     Initializes a new instance of the <see cref="OffsetCheckpointHandler" /> class.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    /// <param name="offsetColumn">The column name used for offset tracking.</param>
    public OffsetCheckpointHandler(CheckpointManager checkpointManager, string offsetColumn)
    {
        ArgumentNullException.ThrowIfNull(checkpointManager);
        ArgumentNullException.ThrowIfNull(offsetColumn);

        // Validate the identifier to prevent SQL injection
        DatabaseIdentifierValidator.ValidateIdentifier(offsetColumn, nameof(offsetColumn));

        _checkpointManager = checkpointManager;
        _offsetColumn = offsetColumn;
        _quotedOffsetColumn = DatabaseIdentifierValidator.QuoteIdentifier(offsetColumn);
    }

    /// <summary>
    ///     Gets the offset column name.
    /// </summary>
    public string OffsetColumn => _offsetColumn;

    /// <summary>
    ///     Gets the current offset value.
    /// </summary>
    /// <returns>The current offset, or 0 if no checkpoint exists.</returns>
    public long GetCurrentOffset()
    {
        return _checkpointManager.GetCurrentOffset() ?? 0;
    }

    /// <summary>
    ///     Loads the checkpoint and returns the offset value.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The offset value, or 0 if no checkpoint exists.</returns>
    public async Task<long> LoadOffsetAsync(CancellationToken cancellationToken = default)
    {
        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken);
        return checkpoint?.GetAsOffset() ?? 0;
    }

    /// <summary>
    ///     Updates the checkpoint with a new offset value.
    /// </summary>
    /// <param name="offset">The new offset value.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateOffsetAsync(long offset, bool forceSave = false, CancellationToken cancellationToken = default)
    {
        var metadata = new Dictionary<string, string>
        {
            ["offset_column"] = _offsetColumn,
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O")
        };

        await _checkpointManager.UpdateOffsetAsync(offset, metadata, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Generates a WHERE clause fragment for filtering based on the current offset.
    /// </summary>
    /// <param name="parameterName">The parameter name to use.</param>
    /// <returns>A tuple containing the WHERE clause and the parameter value.</returns>
    public (string WhereClause, long ParameterValue) GenerateWhereClause(string parameterName = "@offset")
    {
        var offset = GetCurrentOffset();
        var whereClause = offset > 0
            ? $"{_quotedOffsetColumn} > {parameterName}"
            : string.Empty;

        return (whereClause, offset);
    }

    /// <summary>
    ///     Saves the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task SaveAsync(CancellationToken cancellationToken = default)
    {
        await _checkpointManager.SaveAsync(cancellationToken);
    }

    /// <summary>
    ///     Clears the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await _checkpointManager.ClearAsync(cancellationToken);
    }
}
