namespace NPipeline.Connectors.MongoDB.Attributes;

/// <summary>
///     Specifies the MongoDB collection name for a class.
///     Used by convention-based mapping to determine the target collection.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class MongoCollectionAttribute : Attribute
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoCollectionAttribute" /> class.
    /// </summary>
    /// <param name="name">The collection name.</param>
    public MongoCollectionAttribute(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Collection name cannot be empty.", nameof(name));

        Name = name;
    }

    /// <summary>
    ///     Gets the collection name.
    /// </summary>
    public string Name { get; }
}
