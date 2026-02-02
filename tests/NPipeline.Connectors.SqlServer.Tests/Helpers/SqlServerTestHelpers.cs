using Microsoft.Data.SqlClient;

namespace NPipeline.Connectors.SqlServer.Tests.Helpers;

/// <summary>
///     Helper class for SQL Server test database operations.
/// </summary>
public static class SqlServerTestHelpers
{
    /// <summary>
    ///     Creates a test table with standard columns.
    /// </summary>
    public static async Task CreateTestTableAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var createCmd = new SqlCommand(
            $"CREATE TABLE [{schema}].[{tableName}] (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100), Age INT, Email NVARCHAR(255))",
            connection);

        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Creates a test table with custom schema.
    /// </summary>
    public static async Task CreateTestTableWithSchemaAsync(
        string connectionString,
        string tableName,
        string schema,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // Create schema if it doesn't exist
        var createSchemaCmd = new SqlCommand(
            $"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]')",
            connection);

        await createSchemaCmd.ExecuteNonQueryAsync(cancellationToken);

        var createCmd = new SqlCommand(
            $"CREATE TABLE [{schema}].[{tableName}] (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100), Age INT, Email NVARCHAR(255))",
            connection);

        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Creates a test table with identity column.
    /// </summary>
    public static async Task CreateTestTableWithIdentityAsync(
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var createCmd = new SqlCommand(
            $"CREATE TABLE [dbo].[{tableName}] (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100), Age INT, Email NVARCHAR(255))",
            connection);

        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Inserts test data into a table.
    /// </summary>
    public static async Task InsertTestDataAsync(
        string connectionString,
        string tableName,
        int count = 10,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        for (var i = 1; i <= count; i++)
        {
            var insertCmd = new SqlCommand(
                $"INSERT INTO [{schema}].[{tableName}] (Name, Age, Email) VALUES (@Name, @Age, @Email)",
                connection);

            insertCmd.Parameters.AddWithValue("@Name", $"User{i}");
            insertCmd.Parameters.AddWithValue("@Age", 20 + i);
            insertCmd.Parameters.AddWithValue("@Email", $"user{i}@example.com");
            await insertCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    ///     Gets the row count from a table.
    /// </summary>
    public static async Task<int> GetTableRowCountAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var countCmd = new SqlCommand($"SELECT COUNT(*) FROM [{schema}].[{tableName}]", connection);
        var result = await countCmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result);
    }

    /// <summary>
    ///     Gets all data from a table.
    /// </summary>
    public static async Task<List<T>> GetTableDataAsync<T>(
        string connectionString,
        string tableName,
        Func<SqlDataReader, T> mapper,
        string schema = "dbo",
        CancellationToken cancellationToken = default) where T : new()
    {
        var result = new List<T>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var selectCmd = new SqlCommand($"SELECT * FROM [{schema}].[{tableName}] ORDER BY Id", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(mapper(reader));
        }

        return result;
    }

    /// <summary>
    ///     Drops a table if it exists.
    /// </summary>
    public static async Task DropTableAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var dropCmd = new SqlCommand($"DROP TABLE IF EXISTS [{schema}].[{tableName}]", connection);
        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Truncates a table.
    /// </summary>
    public static async Task TruncateTableAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var truncateCmd = new SqlCommand($"TRUNCATE TABLE [{schema}].[{tableName}]", connection);
        await truncateCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Creates a test table with custom columns.
    /// </summary>
    public static async Task CreateTestTableWithColumnsAsync(
        string connectionString,
        string tableName,
        Dictionary<string, string> columns,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var columnDefs = string.Join(", ", columns.Select(kvp => $"[{kvp.Key}] {kvp.Value}"));

        var createCmd = new SqlCommand(
            $"CREATE TABLE [{schema}].[{tableName}] ({columnDefs})",
            connection);

        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Inserts a single row into a table.
    /// </summary>
    public static async Task InsertRowAsync(
        string connectionString,
        string tableName,
        Dictionary<string, object> values,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var columns = string.Join(", ", values.Keys.Select(k => $"[{k}]"));
        var parameters = string.Join(", ", values.Keys.Select(k => $"@{k}"));

        var insertCmd = new SqlCommand(
            $"INSERT INTO [{schema}].[{tableName}] ({columns}) VALUES ({parameters})",
            connection);

        foreach (var kvp in values)
        {
            insertCmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
        }

        await insertCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Executes a custom SQL command.
    /// </summary>
    public static async Task ExecuteCommandAsync(
        string connectionString,
        string command,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var cmd = new SqlCommand(command, connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    ///     Executes a query and returns a scalar value.
    /// </summary>
    public static async Task<T?> ExecuteScalarAsync<T>(
        string connectionString,
        string query,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var cmd = new SqlCommand(query, connection);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);

        if (result == null || result == DBNull.Value)
            return default;

        return (T)Convert.ChangeType(result, typeof(T));
    }

    /// <summary>
    ///     Checks if a table exists.
    /// </summary>
    public static async Task<bool> TableExistsAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var checkCmd = new SqlCommand(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @TableName",
            connection);

        checkCmd.Parameters.AddWithValue("@Schema", schema);
        checkCmd.Parameters.AddWithValue("@TableName", tableName);

        var result = await checkCmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result) > 0;
    }

    /// <summary>
    ///     Gets the column names for a table.
    /// </summary>
    public static async Task<List<string>> GetTableColumnsAsync(
        string connectionString,
        string tableName,
        string schema = "dbo",
        CancellationToken cancellationToken = default)
    {
        var result = new List<string>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var columnsCmd = new SqlCommand(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @TableName ORDER BY ORDINAL_POSITION",
            connection);

        columnsCmd.Parameters.AddWithValue("@Schema", schema);
        columnsCmd.Parameters.AddWithValue("@TableName", tableName);

        await using var reader = await columnsCmd.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    /// <summary>
    ///     Creates a connection string builder with default settings.
    /// </summary>
    public static SqlConnectionStringBuilder CreateConnectionStringBuilder(
        string server,
        string database,
        string username,
        string password,
        bool trustServerCertificate = true,
        int connectTimeout = 30,
        int commandTimeout = 30)
    {
        return new SqlConnectionStringBuilder
        {
            DataSource = server,
            InitialCatalog = database,
            UserID = username,
            Password = password,
            TrustServerCertificate = trustServerCertificate,
            ConnectTimeout = connectTimeout,
            ConnectRetryCount = 3,
            ConnectRetryInterval = 10,
            CommandTimeout = commandTimeout,
            MultipleActiveResultSets = true,
            Encrypt = false,
            IntegratedSecurity = false,
        };
    }

    /// <summary>
    ///     Creates a connection string with integrated security.
    /// </summary>
    public static SqlConnectionStringBuilder CreateIntegratedSecurityConnectionStringBuilder(
        string server,
        string database,
        bool trustServerCertificate = true,
        int connectTimeout = 30,
        int commandTimeout = 30)
    {
        return new SqlConnectionStringBuilder
        {
            DataSource = server,
            InitialCatalog = database,
            TrustServerCertificate = trustServerCertificate,
            ConnectTimeout = connectTimeout,
            ConnectRetryCount = 3,
            ConnectRetryInterval = 10,
            CommandTimeout = commandTimeout,
            MultipleActiveResultSets = true,
            Encrypt = false,
            IntegratedSecurity = true,
        };
    }
}
