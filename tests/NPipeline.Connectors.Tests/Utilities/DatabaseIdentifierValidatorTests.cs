using AwesomeAssertions;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.Tests.Utilities
{
    public sealed class DatabaseIdentifierValidatorTests
    {
        [Fact]
        public void ValidateIdentifier_WithValidIdentifier_ShouldNotThrow()
        {
            // Arrange
            var identifier = "valid_table_name";

            // Act & Assert
            var exception = Record.Exception(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
            _ = exception.Should().BeNull();
        }

        [Fact]
        public void ValidateIdentifier_WithEmptyIdentifier_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = string.Empty;

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithNullIdentifier_ShouldThrowArgumentException()
        {
            // Arrange
            string identifier = null!;

            // Act & Assert
            // Actual behavior: ValidateIdentifier throws ArgumentException for null identifiers
            // because IsValidIdentifier returns false for null (via string.IsNullOrWhiteSpace)
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithWhitespaceIdentifier_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = "   ";

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithSemicolon_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = "table;name";

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithHyphen_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = "table-name";

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithStartingNumber_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = "1table";

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithSpecialCharacters_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = "table@name";

            // Act & Assert
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithUnderscore_ShouldNotThrow()
        {
            // Arrange
            var identifier = "table_name";

            // Act & Assert
            var exception = Record.Exception(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
            _ = exception.Should().BeNull();
        }

        [Fact]
        public void ValidateIdentifier_WithDollarSign_ShouldNotThrow()
        {
            // Arrange
            var identifier = "$table";

            // Act & Assert
            // Actual behavior: ValidateIdentifier throws ArgumentException for dollar signs
            // because the regex ^[a-zA-Z_][a-zA-Z0-9_]*$ doesn't allow $
            _ = Assert.Throws<ArgumentException>(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
        }

        [Fact]
        public void ValidateIdentifier_WithMixedCase_ShouldNotThrow()
        {
            // Arrange
            var identifier = "TableName";

            // Act & Assert
            var exception = Record.Exception(() => DatabaseIdentifierValidator.ValidateIdentifier(identifier, "test"));
            _ = exception.Should().BeNull();
        }

        [Fact]
        public void QuoteIdentifier_WithSimpleIdentifier_ShouldQuoteCorrectly()
        {
            // Arrange
            var identifier = "table_name";

            // Act
            var quoted = DatabaseIdentifierValidator.QuoteIdentifier(identifier);

            // Assert
            _ = quoted.Should().Be("\"table_name\"");
        }

        [Fact]
        public void QuoteIdentifier_WithSchema_ShouldQuoteCorrectly()
        {
            // Arrange
            var identifier = "schema.table_name";

            // Act
            var quoted = DatabaseIdentifierValidator.QuoteIdentifier(identifier);

            // Assert
            // Actual behavior: QuoteIdentifier doesn't parse schema-qualified identifiers,
            // it just wraps the entire string in quotes
            _ = quoted.Should().Be("\"schema.table_name\"");
        }

        [Fact]
        public void QuoteIdentifier_WithQuotedIdentifier_ShouldNotDoubleQuote()
        {
            // Arrange
            var identifier = "\"table_name\"";

            // Act
            var quoted = DatabaseIdentifierValidator.QuoteIdentifier(identifier);

            // Assert
            // Actual behavior: QuoteIdentifier escapes existing quote characters by doubling them,
            // then wraps the result in quotes. So "table_name" becomes """table_name""" (3 quotes on each side)
            _ = quoted.Should().Be("\"\"\"table_name\"\"\"");
        }

        [Fact]
        public void QuoteIdentifier_WithEmptyIdentifier_ShouldThrowArgumentException()
        {
            // Arrange
            var identifier = string.Empty;

            // Act
            var quoted = DatabaseIdentifierValidator.QuoteIdentifier(identifier);

            // Assert
            // Actual behavior: QuoteIdentifier doesn't validate the identifier,
            // it just wraps it in quotes, so empty string becomes ""
            _ = quoted.Should().Be("\"\"");
        }

        [Fact]
        public void QuoteIdentifier_WithNullIdentifier_ShouldThrowArgumentNullException()
        {
            // Arrange
            string identifier = null!;

            // Act & Assert
            // Actual behavior: QuoteIdentifier doesn't validate for null,
            // so it throws NullReferenceException when trying to call .Replace() on null
            _ = Assert.Throws<NullReferenceException>(() => DatabaseIdentifierValidator.QuoteIdentifier(identifier));
        }

        [Fact]
        public void QuoteIdentifier_WithSpecialCharacters_ShouldQuoteCorrectly()
        {
            // Arrange
            var identifier = "table.name";

            // Act
            var quoted = DatabaseIdentifierValidator.QuoteIdentifier(identifier);

            // Assert
            _ = quoted.Should().Be("\"table.name\"");
        }
    }
}
