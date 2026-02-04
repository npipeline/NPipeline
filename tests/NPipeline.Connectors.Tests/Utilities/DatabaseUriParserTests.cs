using NPipeline.Connectors;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.Tests.Utilities
{
    public sealed class DatabaseUriParserTests
    {
        // URI Parsing Tests

        [Fact]
        public void Parse_WithQueryParameters_ExtractsCorrectValues()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb?username=testuser&password=testpass");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal("testuser", info.Username);
            Assert.Equal("testpass", info.Password);
            Assert.Null(info.Port);
        }

        [Fact]
        public void Parse_WithUserInfo_ExtractsCorrectValues()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://testuser:testpass@localhost/mydb");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal("testuser", info.Username);
            Assert.Equal("testpass", info.Password);
            Assert.Null(info.Port);
        }

        [Fact]
        public void Parse_WithPortFromUri_ExtractsCorrectValues()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost:5432/mydb");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal(5432, info.Port);
        }

        [Fact]
        public void Parse_WithPortFromQueryParameter_ExtractsCorrectValues()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb?port=5433");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal(5433, info.Port);
        }

        [Fact]
        public void Parse_WithDatabaseFromPath_ExtractsCorrectValues()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydatabase");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydatabase", info.Database);
        }

        [Fact]
        public void Parse_WithUrlEncodedPassword_DecodesCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://testuser:p%40ss%23w%24rd@localhost/mydb");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal("testuser", info.Username);
            Assert.Equal("p@ss#w$rd", info.Password);
        }

        [Fact]
        public void Parse_WithSslModeParameter_ExtractsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb?sslmode=require");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.True(info.Parameters.ContainsKey("sslmode"));
            Assert.Equal("require", info.Parameters["sslmode"]);
        }

        [Fact]
        public void Parse_WithNullHost_ThrowsArgumentException()
        {
            // Arrange - Use a URI with empty host (triple slash)
            var uri = StorageUri.Parse("postgres:///mydb");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => DatabaseUriParser.Parse(uri));
        }

        [Fact]
        public void Parse_WithNullDatabase_ThrowsArgumentException()
        {
            // Arrange - Use a URI with empty path (just trailing slash)
            var uri = StorageUri.Parse("postgres://localhost/");

            // Act & Assert
            Assert.Throws<ArgumentException>(() => DatabaseUriParser.Parse(uri));
        }

        [Fact]
        public void Parse_WithInvalidPortValue_ThrowsException()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb?port=invalid");

            // Act & Assert
            // The parser should not throw for invalid port, it should just return null
            var info = DatabaseUriParser.Parse(uri);
            Assert.Null(info.Port);
        }

        // Connection String Building Tests

        [Fact]
        public void BuildConnectionString_WithAllComponents_GeneratesCorrectString()
        {
            // Arrange
            var info = new DatabaseConnectionInfo(
                Host: "localhost",
                Port: 5432,
                Database: "mydb",
                Username: "testuser",
                Password: "testpass",
                Parameters: new Dictionary<string, string>
                {
                    { "sslmode", "require" },
                    { "timeout", "30" }
                }
            );

            // Act
            var connectionString = DatabaseUriParser.BuildConnectionString(info);

            // Assert
            Assert.Contains("Host=localhost", connectionString);
            Assert.Contains("Port=5432", connectionString);
            Assert.Contains("Database=mydb", connectionString);
            Assert.Contains("Username=testuser", connectionString);
            Assert.Contains("Password=testpass", connectionString);
            Assert.Contains("sslmode=require", connectionString);
            Assert.Contains("timeout=30", connectionString);
        }

        [Fact]
        public void BuildConnectionString_WithOptionalComponentsOmitted_GeneratesCorrectString()
        {
            // Arrange
            var info = new DatabaseConnectionInfo(
                Host: "localhost",
                Port: null,
                Database: "mydb",
                Username: null,
                Password: null,
                Parameters: new Dictionary<string, string>()
            );

            // Act
            var connectionString = DatabaseUriParser.BuildConnectionString(info);

            // Assert
            Assert.Contains("Host=localhost", connectionString);
            Assert.Contains("Database=mydb", connectionString);
            Assert.DoesNotContain("Port=", connectionString);
            Assert.DoesNotContain("Username=", connectionString);
            Assert.DoesNotContain("Password=", connectionString);
        }

        [Fact]
        public void BuildConnectionString_WithAdditionalParameters_IncludesParameters()
        {
            // Arrange
            var info = new DatabaseConnectionInfo(
                Host: "localhost",
                Port: null,
                Database: "mydb",
                Username: null,
                Password: null,
                Parameters: new Dictionary<string, string>
                {
                    { "sslmode", "require" },
                    { "connect_timeout", "10" },
                    { "command_timeout", "30" }
                }
            );

            // Act
            var connectionString = DatabaseUriParser.BuildConnectionString(info);

            // Assert
            Assert.Contains("sslmode=require", connectionString);
            Assert.Contains("connect_timeout=10", connectionString);
            Assert.Contains("command_timeout=30", connectionString);
        }

        [Fact]
        public void BuildConnectionString_WithUrlEncodedPassword_HandlesCorrectly()
        {
            // Arrange
            var info = new DatabaseConnectionInfo(
                Host: "localhost",
                Port: null,
                Database: "mydb",
                Username: "testuser",
                Password: "p@ss#w$rd",
                Parameters: new Dictionary<string, string>()
            );

            // Act
            var connectionString = DatabaseUriParser.BuildConnectionString(info);

            // Assert
            Assert.Contains("Host=localhost", connectionString);
            Assert.Contains("Database=mydb", connectionString);
            Assert.Contains("Username=testuser", connectionString);
            Assert.Contains("Password=p@ss#w$rd", connectionString);
        }

        [Fact]
        public void BuildConnectionString_ExcludesHandledParameters()
        {
            // Arrange
            var info = new DatabaseConnectionInfo(
                Host: "localhost",
                Port: 5432,
                Database: "mydb",
                Username: "testuser",
                Password: "testpass",
                Parameters: new Dictionary<string, string>
                {
                    { "username", "otheruser" },
                    { "password", "otherpass" },
                    { "port", "9999" },
                    { "sslmode", "require" }
                }
            );

            // Act
            var connectionString = DatabaseUriParser.BuildConnectionString(info);

            // Assert
            Assert.Contains("Host=localhost", connectionString);
            Assert.Contains("Port=5432", connectionString);
            Assert.Contains("Database=mydb", connectionString);
            Assert.Contains("Username=testuser", connectionString);
            Assert.Contains("Password=testpass", connectionString);
            Assert.Contains("sslmode=require", connectionString);
            // Handled parameters should not appear in the connection string
            Assert.DoesNotContain("username=otheruser", connectionString);
            Assert.DoesNotContain("password=otherpass", connectionString);
            Assert.DoesNotContain("port=9999", connectionString);
        }

        [Fact]
        public void BuildConnectionString_WithNullInfo_ThrowsArgumentNullException()
        {
            // Arrange
            DatabaseConnectionInfo? info = null;

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => DatabaseUriParser.BuildConnectionString(info!));
        }

        [Fact]
        public void Parse_WithAlternateCredentialParameters_ExtractsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb?user=altuser&pwd=altpass");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            Assert.Equal("altuser", info.Username);
            Assert.Equal("altpass", info.Password);
        }

        [Fact]
        public void Parse_WithQueryParametersPreferredOverUserInfo_ExtractsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://userinfo:infopass@localhost/mydb?username=queryuser&password=querypass");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            // Query parameters should take precedence
            Assert.Equal("queryuser", info.Username);
            Assert.Equal("querypass", info.Password);
        }

        [Fact]
        public void Parse_WithPortFromUriPreferredOverQueryParameter_ExtractsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost:5432/mydb?port=5433");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
            // URI port should take precedence
            Assert.Equal(5432, info.Port);
        }

        [Fact]
        public void Parse_WithDatabasePathWithLeadingSlash_TrimsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost//mydb");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
        }

        [Fact]
        public void Parse_WithDatabasePathWithTrailingSlash_TrimsCorrectly()
        {
            // Arrange
            var uri = StorageUri.Parse("postgres://localhost/mydb/");

            // Act
            var info = DatabaseUriParser.Parse(uri);

            // Assert
            Assert.Equal("localhost", info.Host);
            Assert.Equal("mydb", info.Database);
        }
    }
}
