namespace NPipeline.Connectors.PostgreSQL.Mapping
{
    /// <summary>
    /// Specifies the PostgreSQL table name and schema for a class.
    /// Used by convention-based mapping to determine the target table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public sealed class PostgresTableAttribute : Attribute
    {
        /// <summary>
        /// Gets the table name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets the schema name.
        /// Default is "public".
        /// </summary>
        public string Schema { get; set; } = "public";

        /// <summary>
        /// Initializes a new instance of the PostgresTableAttribute.
        /// </summary>
        /// <param name="name">The table name.</param>
        public PostgresTableAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Table name cannot be empty.", nameof(name));
            }

            Name = name;
        }
    }
}
