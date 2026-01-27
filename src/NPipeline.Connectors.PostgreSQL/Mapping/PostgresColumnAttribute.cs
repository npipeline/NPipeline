using NpgsqlTypes;

namespace NPipeline.Connectors.PostgreSQL.Mapping
{
    /// <summary>
    /// Specifies the PostgreSQL column mapping for a property or field.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class PostgresColumnAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresColumnAttribute"/> class.
        /// </summary>
        /// <param name="name">The column name.</param>
        public PostgresColumnAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Column name cannot be empty.", nameof(name));
            }

            Name = name;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets the PostgreSQL database type.
        /// </summary>
        public NpgsqlDbType? DbType { get; set; }

        /// <summary>
        /// Gets or sets the column size (for variable-length types).
        /// </summary>
        public int? Size { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the column participates in the primary key.
        /// </summary>
        public bool PrimaryKey { get; set; }

        /// <summary>
        /// Gets or sets whether this column should be ignored during mapping.
        /// </summary>
        public bool Ignore { get; set; }
    }
}
