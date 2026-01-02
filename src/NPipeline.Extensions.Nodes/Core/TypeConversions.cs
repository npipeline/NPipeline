using System.Globalization;
using NPipeline.Extensions.Nodes.Core.Exceptions;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     Factory methods for creating <see cref="TypeConversionNode{TIn, TOut}" /> instances with common conversions.
/// </summary>
public static class TypeConversions
{
    /// <summary>
    ///     Converts a string input to an integer.
    /// </summary>
    public static TypeConversionNode<string, int> StringToInt(
        NumberStyles style = NumberStyles.Integer,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<string, int>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(int), input, "Input string is null or empty.");

            if (int.TryParse(input, style, formatProvider, out var result))
                return result;

            throw new TypeConversionException(typeof(string), typeof(int), input, $"Cannot convert '{input}' to int.");
        });
    }

    /// <summary>
    ///     Converts a string input to a long.
    /// </summary>
    public static TypeConversionNode<string, long> StringToLong(
        NumberStyles style = NumberStyles.Integer,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<string, long>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(long), input, "Input string is null or empty.");

            if (long.TryParse(input, style, formatProvider, out var result))
                return result;

            throw new TypeConversionException(typeof(string), typeof(long), input, $"Cannot convert '{input}' to long.");
        });
    }

    /// <summary>
    ///     Converts a string input to a double.
    /// </summary>
    public static TypeConversionNode<string, double> StringToDouble(
        NumberStyles style = NumberStyles.Float | NumberStyles.AllowThousands,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<string, double>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(double), input, "Input string is null or empty.");

            if (double.TryParse(input, style, formatProvider, out var result))
                return result;

            throw new TypeConversionException(typeof(string), typeof(double), input, $"Cannot convert '{input}' to double.");
        });
    }

    /// <summary>
    ///     Converts a string input to a decimal.
    /// </summary>
    public static TypeConversionNode<string, decimal> StringToDecimal(
        NumberStyles style = NumberStyles.Number,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<string, decimal>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(decimal), input, "Input string is null or empty.");

            if (decimal.TryParse(input, style, formatProvider, out var result))
                return result;

            throw new TypeConversionException(typeof(string), typeof(decimal), input, $"Cannot convert '{input}' to decimal.");
        });
    }

    /// <summary>
    ///     Converts a string input to a boolean.
    /// </summary>
    public static TypeConversionNode<string, bool> StringToBool()
    {
        return new TypeConversionNode<string, bool>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(bool), input, "Input string is null or empty.");

            return input.Trim().ToLowerInvariant() switch
            {
                "true" or "1" or "yes" or "on" => true,
                "false" or "0" or "no" or "off" => false,
                _ => throw new TypeConversionException(typeof(string), typeof(bool), input,
                    $"Cannot convert '{input}' to bool. Valid values: true/false, yes/no, on/off, 1/0."),
            };
        });
    }

    /// <summary>
    ///     Converts a string input to a DateTime with optional format and format provider.
    /// </summary>
    public static TypeConversionNode<string, DateTime> StringToDateTime(
        string? format = null,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<string, DateTime>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(DateTime), input, "Input string is null or empty.");

            if (format != null)
            {
                if (DateTime.TryParseExact(input, format, formatProvider, DateTimeStyles.None, out var result))
                    return result;
            }
            else
            {
                if (DateTime.TryParse(input, formatProvider, DateTimeStyles.None, out var result))
                    return result;
            }

            var formatMsg = format != null
                ? $" using format '{format}'"
                : "";

            throw new TypeConversionException(typeof(string), typeof(DateTime), input,
                $"Cannot convert '{input}' to DateTime{formatMsg}.");
        });
    }

    /// <summary>
    ///     Converts an integer to a string using optional format and format provider.
    /// </summary>
    public static TypeConversionNode<int, string> IntToString(
        string? format = null,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<int, string>().WithConverter(input =>
            format != null
                ? input.ToString(format, formatProvider)
                : input.ToString(formatProvider));
    }

    /// <summary>
    ///     Converts a double to a string using optional format and format provider.
    /// </summary>
    public static TypeConversionNode<double, string> DoubleToString(
        string? format = null,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<double, string>().WithConverter(input =>
            format != null
                ? input.ToString(format, formatProvider)
                : input.ToString(formatProvider));
    }

    /// <summary>
    ///     Converts a decimal to a string using optional format and format provider.
    /// </summary>
    public static TypeConversionNode<decimal, string> DecimalToString(
        string? format = null,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<decimal, string>().WithConverter(input =>
            format != null
                ? input.ToString(format, formatProvider)
                : input.ToString(formatProvider));
    }

    /// <summary>
    ///     Converts a DateTime to a string using optional format and format provider.
    /// </summary>
    public static TypeConversionNode<DateTime, string> DateTimeToString(
        string? format = null,
        IFormatProvider? formatProvider = null)
    {
        formatProvider ??= CultureInfo.InvariantCulture;

        return new TypeConversionNode<DateTime, string>().WithConverter(input =>
            format != null
                ? input.ToString(format, formatProvider)
                : input.ToString(formatProvider));
    }

    /// <summary>
    ///     Converts a boolean to a string using custom true/false representations.
    /// </summary>
    public static TypeConversionNode<bool, string> BoolToString(
        string trueValue = "true",
        string falseValue = "false")
    {
        ArgumentNullException.ThrowIfNull(trueValue);
        ArgumentNullException.ThrowIfNull(falseValue);

        return new TypeConversionNode<bool, string>().WithConverter(input =>
            input
                ? trueValue
                : falseValue);
    }

    /// <summary>
    ///     Converts an enum to its string representation.
    /// </summary>
    public static TypeConversionNode<TEnum, string> EnumToString<TEnum>() where TEnum : struct, Enum
    {
        return new TypeConversionNode<TEnum, string>().WithConverter(input =>
            input.ToString());
    }

    /// <summary>
    ///     Converts a string to an enum value.
    /// </summary>
    public static TypeConversionNode<string, TEnum> StringToEnum<TEnum>(bool ignoreCase = true) where TEnum : struct, Enum
    {
        return new TypeConversionNode<string, TEnum>().WithConverter(input =>
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new TypeConversionException(typeof(string), typeof(TEnum), input, "Input string is null or empty.");

            if (Enum.TryParse<TEnum>(input, ignoreCase, out var result))
                return result;

            throw new TypeConversionException(typeof(string), typeof(TEnum), input,
                $"Cannot convert '{input}' to {typeof(TEnum).Name}.");
        });
    }
}
