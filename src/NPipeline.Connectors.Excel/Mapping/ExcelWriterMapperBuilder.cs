using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Spreadsheet;
using NPipeline.Connectors.Excel.Attributes;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Builds property mapping delegates for writing objects to Excel rows.
/// </summary>
public static class ExcelWriterMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();
    private static readonly ConcurrentDictionary<Type, string[]> ColumnNamesCache = new();
    private static readonly ConcurrentDictionary<Type, object> GetterCache = new();

    /// <summary>
    ///     Builds a cached mapping delegate for writing objects of type T to Excel rows.
    /// </summary>
    /// <typeparam name="T">The type of objects to write to Excel.</typeparam>
    /// <returns>A delegate that writes an instance of type T to an <see cref="OpenXmlWriter" />.</returns>
    public static Action<OpenXmlWriter, T> Build<T>()
    {
        var type = typeof(T);

        if (MapperCache.TryGetValue(type, out var cachedDelegate))
            return (Action<OpenXmlWriter, T>)cachedDelegate;

        var mapper = BuildMapper<T>();
        MapperCache.TryAdd(type, mapper);
        return mapper;
    }

    /// <summary>
    ///     Gets the column names for type T based on property mappings.
    /// </summary>
    /// <typeparam name="T">The type to get column names for.</typeparam>
    /// <returns>An array of column names for the type.</returns>
    public static string[] GetColumnNames<T>()
    {
        var type = typeof(T);

        if (ColumnNamesCache.TryGetValue(type, out var cachedColumnNames))
            return cachedColumnNames;

        var columnNames = BuildColumnNames<T>();
        ColumnNamesCache.TryAdd(type, columnNames);
        return columnNames;
    }

    /// <summary>
    ///     Gets cached compiled property getters for type T.
    /// </summary>
    /// <typeparam name="T">The type to get getters for.</typeparam>
    /// <returns>An array of compiled getters aligned with column order.</returns>
    public static Func<T, object?>[] GetValueGetters<T>()
    {
        var type = typeof(T);

        if (GetterCache.TryGetValue(type, out var cachedGetters))
            return (Func<T, object?>[])cachedGetters;

        var getters = BuildValueGetters<T>();
        GetterCache.TryAdd(type, getters);
        return getters;
    }

    private static Action<OpenXmlWriter, T> BuildMapper<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<ExcelColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                Getter = BuildGetter<T>(p.Property),
            })
            .ToList();

        return (writer, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item);
                WriteCell(writer, value);
            }
        };
    }

    private static Func<T, object?>[] BuildValueGetters<T>()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(BuildGetter<T>),
        ];
    }

    private static string[] BuildColumnNames<T>()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => p.GetCustomAttribute<ExcelColumnAttribute>()?.Name ?? ToColumnNameConvention(p.Name)),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ExcelColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(ExcelIgnoreAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static Func<T, object?> BuildGetter<T>(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }

    /// <summary>
    ///     Converts a PascalCase property name to an Excel column name convention.
    ///     Converts to lowercase by default (e.g., "FirstName" -> "firstname").
    /// </summary>
    private static string ToColumnNameConvention(string str)
    {
        return str.ToLowerInvariant();
    }

    private static void WriteCell(OpenXmlWriter writer, object? value)
    {
        var (cell, inlineString) = CreateCellValue(value);

        writer.WriteStartElement(cell);

        if (inlineString is not null)
            writer.WriteElement(inlineString);
        else if (cell.CellValue is not null)
            writer.WriteElement(cell.CellValue);

        writer.WriteEndElement();
    }

    private static (Cell Cell, InlineString? InlineString) CreateCellValue(object? value)
    {
        var cell = new Cell();

        if (value is null || value == DBNull.Value)
            return (cell, null);

        switch (value)
        {
            case string s:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(s)));

            case bool b:
                cell.DataType = CellValues.Boolean;

                cell.CellValue = new CellValue(b
                    ? "1"
                    : "0");

                return (cell, null);

            case DateTime dt:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(dt.ToOADate().ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case DateTimeOffset dto:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(dto.UtcDateTime.ToOADate().ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case int i:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(i.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case long l:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(l.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case short s16:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(s16.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case decimal m:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(m.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case double d:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(d.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case float f:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(f.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case uint ui:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(ui.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case ulong ul:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(ul.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case ushort us:
                cell.DataType = CellValues.Number;
                cell.CellValue = new CellValue(us.ToString(CultureInfo.InvariantCulture));
                return (cell, null);

            case Enum e:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(e.ToString())));

            default:
                cell.DataType = CellValues.InlineString;
                return (cell, new InlineString(new Text(value.ToString() ?? string.Empty)));
        }
    }
}
