using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.MongoDB.Attributes;

/// <summary>
///     Specifies the MongoDB field mapping for a property.
///     Inherits from <see cref="ColumnAttribute" /> for consistency with other connectors.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class MongoFieldAttribute : ColumnAttribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoFieldAttribute" /> class.
    /// </summary>
    /// <param name="name">The field name in MongoDB.</param>
    public MongoFieldAttribute(string name) : base(name)
    {
    }
}
