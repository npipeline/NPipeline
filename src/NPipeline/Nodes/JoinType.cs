namespace NPipeline.Nodes;

/// <summary>
///     Specifies the type of join operation to perform between two streams.
/// </summary>
public enum JoinType
{
    /// <summary>
    ///     An inner join produces results only when there is a match in both streams.
    /// </summary>
    Inner,

    /// <summary>
    ///     A left outer join produces all results from the left stream, and matching results from the right stream.
    ///     When there is no match in the right stream, the result contains the left item with default values for the right side.
    /// </summary>
    LeftOuter,

    /// <summary>
    ///     A right outer join produces all results from the right stream, and matching results from the left stream.
    ///     When there is no match in the left stream, the result contains the right item with default values for the left side.
    /// </summary>
    RightOuter,

    /// <summary>
    ///     A full outer join produces all results from both streams.
    ///     When there is no match in one stream, the result contains the item from the other stream with default values for the missing side.
    /// </summary>
    FullOuter,
}
