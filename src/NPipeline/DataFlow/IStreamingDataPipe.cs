namespace NPipeline.DataFlow;

/// <summary>
///     A marker interface to identify data pipes that stream their data and may not be replayable.
/// </summary>
public interface IStreamingDataPipe : IDataPipe
{
}
