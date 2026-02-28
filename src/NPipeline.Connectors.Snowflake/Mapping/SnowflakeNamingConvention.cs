using System.Text;

namespace NPipeline.Connectors.Snowflake.Mapping;

internal static class SnowflakeNamingConvention
{
    public static string ToDefaultColumnName(string propertyName)
    {
        if (string.IsNullOrWhiteSpace(propertyName))
            return propertyName;

        var builder = new StringBuilder(propertyName.Length + 8);

        for (var i = 0; i < propertyName.Length; i++)
        {
            var current = propertyName[i];

            if (!char.IsLetterOrDigit(current))
            {
                if (builder.Length > 0 && builder[^1] != '_')
                    builder.Append('_');

                continue;
            }

            if (char.IsUpper(current) && i > 0)
            {
                var previous = propertyName[i - 1];
                var hasNext = i + 1 < propertyName.Length;
                var next = hasNext ? propertyName[i + 1] : '\0';

                var startsNewWord = char.IsLower(previous)
                                    || char.IsDigit(previous)
                                    || (char.IsUpper(previous) && hasNext && char.IsLower(next));

                if (startsNewWord && builder.Length > 0 && builder[^1] != '_')
                    builder.Append('_');
            }
            else if (char.IsDigit(current) && i > 0)
            {
                var previous = propertyName[i - 1];

                if (char.IsLetter(previous) && builder.Length > 0 && builder[^1] != '_')
                    builder.Append('_');
            }

            builder.Append(char.ToUpperInvariant(current));
        }

        return builder.ToString();
    }
}