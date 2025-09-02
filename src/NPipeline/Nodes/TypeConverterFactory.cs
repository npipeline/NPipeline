using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;

namespace NPipeline.Nodes;

internal sealed class TypeConverterFactory
{
    private readonly ConcurrentDictionary<(Type Src, Type Dest), Delegate> _converters = new();

    public static TypeConverterFactory CreateDefault()
    {
        var f = new TypeConverterFactory();

        // Identity is handled on-demand, not pre-registered.

        // string -> primitives
        f.Register<string, string>(s => s);
        f.Register<string, int>(s => int.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
        f.Register<string, long>(s => long.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
        f.Register<string, short>(s => short.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
        f.Register<string, byte>(s => byte.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
        f.Register<string, float>(s => float.Parse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture));
        f.Register<string, double>(s => double.Parse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture));
        f.Register<string, decimal>(s => decimal.Parse(s, NumberStyles.Number, CultureInfo.InvariantCulture));
        f.Register<string, bool>(s => bool.Parse(s));
        f.Register<string, Guid>(s => Guid.Parse(s));
        f.Register<string, TimeSpan>(s => TimeSpan.Parse(s, CultureInfo.InvariantCulture));
        f.Register<string, DateTime>(s => DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));

        f.Register<string, DateTimeOffset>(s =>
            DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));

        // Numeric widenings via Convert.ChangeType handler (generated on demand)

        return f;
    }

    public void Register<TSrc, TDest>(Func<TSrc, TDest> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        _converters[(typeof(TSrc), typeof(TDest))] = converter;
    }

    public bool TryGetConverter<TSrc, TDest>(out Func<TSrc, TDest> converter)
    {
        if (TryGetConverter(typeof(TSrc), typeof(TDest), out var del))
        {
            converter = (Func<TSrc, TDest>)del;
            return true;
        }

        converter = default!;
        return false;
    }

    public bool TryGetConverter(Type srcType, Type destType, out Delegate converter)
    {
        ArgumentNullException.ThrowIfNull(srcType);
        ArgumentNullException.ThrowIfNull(destType);

        // Exact registration
        if (_converters.TryGetValue((srcType, destType), out var existing))
        {
            converter = existing;
            return true;
        }

        // Identity
        if (destType.IsAssignableFrom(srcType))
        {
            converter = BuildIdentity(srcType, destType);
            _converters[(srcType, destType)] = converter;
            return true;
        }

        // Nullable<TDest> wrapper
        if (IsNullable(destType, out var destUnderlying))
        {
            // If string -> Nullable<T>, treat null/empty/whitespace as null.
            if (srcType == typeof(string))
            {
                if (TryGetConverter(srcType, destUnderlying, out var inner))
                {
                    var wrapped = BuildNullableFromStringWrapper(destUnderlying, inner);
                    converter = wrapped;
                    _converters[(srcType, destType)] = converter;
                    return true;
                }

                if (TryBuildWellKnown(srcType, destUnderlying, out inner))
                {
                    var wrapped = BuildNullableFromStringWrapper(destUnderlying, inner);
                    converter = wrapped;
                    _converters[(srcType, destType)] = converter;
                    return true;
                }
            }
            else
            {
                if (TryGetConverter(srcType, destUnderlying, out var inner) || TryBuildWellKnown(srcType, destUnderlying, out inner))
                {
                    var wrapped = BuildNullableWrapper(srcType, destUnderlying, inner);
                    converter = wrapped;
                    _converters[(srcType, destType)] = converter;
                    return true;
                }
            }
        }

        // Enums
        if (destType.IsEnum)
        {
            // string -> enum
            if (srcType == typeof(string))
            {
                converter = BuildEnumStringParser(destType);
                _converters[(srcType, destType)] = converter;
                return true;
            }

            // numeric -> enum (change type to underlying, then Enum.ToObject)
            if (IsNumeric(srcType))
            {
                converter = BuildNumericToEnum(srcType, destType);
                _converters[(srcType, destType)] = converter;
                return true;
            }
        }

        // Numeric widenings/changes via Convert.ChangeType for numeric pairs
        if (IsNumeric(srcType) && IsNumeric(destType))
        {
            converter = BuildNumericConverter(srcType, destType);
            _converters[(srcType, destType)] = converter;
            return true;
        }

        // Try build other well-known pairs (string -> numeric/date/time/guid) if not registered explicitly
        if (TryBuildWellKnown(srcType, destType, out var built))
        {
            converter = built;
            _converters[(srcType, destType)] = converter;
            return true;
        }

        converter = default!;
        return false;
    }

    private static bool IsNullable(Type t, [NotNullWhen(true)] out Type? underlying)
    {
        underlying = Nullable.GetUnderlyingType(t);
        return underlying is not null;
    }

    private static bool IsNumeric(Type t)
    {
        var type = t;

        if (IsNullable(t, out var u))
            type = u;

        return type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(short) || type == typeof(ushort) ||
               type == typeof(int) || type == typeof(uint) ||
               type == typeof(long) || type == typeof(ulong) ||
               type == typeof(float) || type == typeof(double) ||
               type == typeof(decimal);
    }

    private static Delegate BuildIdentity(Type src, Type dest)
    {
        // (TSrc v) => (TDest)v
        var p = Expression.Parameter(src, "v");

        var body = src == dest
            ? (Expression)p
            : Expression.Convert(p, dest);

        var lambdaType = typeof(Func<,>).MakeGenericType(src, dest);
        return Expression.Lambda(lambdaType, body, p).Compile();
    }

    private static bool TryBuildWellKnown(Type src, Type dest, out Delegate converter)
    {
        converter = default!;

        // string -> enum (already handled above)
        if (src == typeof(string))
        {
            if (dest == typeof(Guid))
            {
                converter = (Func<string, Guid>)(s => Guid.Parse(s));
                return true;
            }

            if (dest == typeof(TimeSpan))
            {
                converter = (Func<string, TimeSpan>)(s => TimeSpan.Parse(s, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(DateTime))
            {
                converter = (Func<string, DateTime>)(s =>
                    DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));

                return true;
            }

            if (dest == typeof(DateTimeOffset))
            {
                converter = (Func<string, DateTimeOffset>)(s =>
                    DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal));

                return true;
            }

            if (dest == typeof(bool))
            {
                converter = (Func<string, bool>)(s => bool.Parse(s));
                return true;
            }

            if (dest == typeof(int))
            {
                converter = (Func<string, int>)(s => int.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(long))
            {
                converter = (Func<string, long>)(s => long.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(short))
            {
                converter = (Func<string, short>)(s => short.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(byte))
            {
                converter = (Func<string, byte>)(s => byte.Parse(s, NumberStyles.Integer, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(float))
            {
                converter = (Func<string, float>)(s => float.Parse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(double))
            {
                converter = (Func<string, double>)(s => double.Parse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest == typeof(decimal))
            {
                converter = (Func<string, decimal>)(s => decimal.Parse(s, NumberStyles.Number, CultureInfo.InvariantCulture));
                return true;
            }

            if (dest.IsEnum)
            {
                converter = BuildEnumStringParser(dest);
                return true;
            }
        }

        return false;
    }

    private static Delegate BuildEnumStringParser(Type enumType)
    {
        var mi = typeof(Enum).GetMethod(nameof(Enum.Parse), [typeof(Type), typeof(string), typeof(bool)])!;

        // (string s) => (TEnum)Enum.Parse(typeof(TEnum), s, true)
        var s = Expression.Parameter(typeof(string), "s");

        var call = Expression.Call(mi,
            Expression.Constant(enumType, typeof(Type)),
            s,
            Expression.Constant(true, typeof(bool)));

        var body = Expression.Convert(call, enumType);
        var lambdaType = typeof(Func<,>).MakeGenericType(typeof(string), enumType);
        return Expression.Lambda(lambdaType, body, s).Compile();
    }

    private static Delegate BuildNumericToEnum(Type srcType, Type enumType)
    {
        var underlying = Enum.GetUnderlyingType(enumType);

        // (TSrc v) => (TEnum) Enum.ToObject(typeof(TEnum), Convert.ChangeType(v, underlying, InvariantCulture))
        var v = Expression.Parameter(srcType, "v");
        var toObj = Expression.Convert(v, typeof(object));

        var changeType = Expression.Call(
            typeof(Convert).GetMethod(nameof(Convert.ChangeType), [typeof(object), typeof(Type), typeof(IFormatProvider)])!,
            toObj,
            Expression.Constant(underlying, typeof(Type)),
            Expression.Constant(CultureInfo.InvariantCulture, typeof(IFormatProvider)));

        var enumToObj = Expression.Call(
            typeof(Enum).GetMethod(nameof(Enum.ToObject), [typeof(Type), typeof(object)])!,
            Expression.Constant(enumType, typeof(Type)),
            changeType);

        var body = Expression.Convert(enumToObj, enumType);
        var lambdaType = typeof(Func<,>).MakeGenericType(srcType, enumType);
        return Expression.Lambda(lambdaType, body, v).Compile();
    }

    private static Delegate BuildNumericConverter(Type srcType, Type destType)
    {
        // (TSrc v) => (TDest) Convert.ChangeType(v, typeof(TDest), InvariantCulture)
        var v = Expression.Parameter(srcType, "v");
        var toObj = Expression.Convert(v, typeof(object));

        var call = Expression.Call(
            typeof(Convert).GetMethod(nameof(Convert.ChangeType), [typeof(object), typeof(Type), typeof(IFormatProvider)])!,
            toObj,
            Expression.Constant(destType, typeof(Type)),
            Expression.Constant(CultureInfo.InvariantCulture, typeof(IFormatProvider)));

        var body = Expression.Convert(call, destType);
        var lambdaType = typeof(Func<,>).MakeGenericType(srcType, destType);
        return Expression.Lambda(lambdaType, body, v).Compile();
    }

    private static Delegate BuildNullableWrapper(Type srcType, Type destUnderlying, Delegate inner)
    {
        // (TSrc v) => (TDest?) inner(v)
        var destNullable = typeof(Nullable<>).MakeGenericType(destUnderlying);
        var p = Expression.Parameter(srcType, "v");
        var innerInvoke = Expression.Invoke(Expression.Constant(inner), p);
        var body = Expression.Convert(innerInvoke, destNullable);
        var lambdaType = typeof(Func<,>).MakeGenericType(srcType, destNullable);
        return Expression.Lambda(lambdaType, body, p).Compile();
    }

    private static Delegate BuildNullableFromStringWrapper(Type destUnderlying, Delegate inner)
    {
        // (string s) => string.IsNullOrWhiteSpace(s) ? default(T?) : (T?)inner(s)
        var destNullable = typeof(Nullable<>).MakeGenericType(destUnderlying);
        var s = Expression.Parameter(typeof(string), "s");

        var isNullOrWhiteSpace = Expression.Call(
            typeof(string).GetMethod(nameof(string.IsNullOrWhiteSpace), [typeof(string)])!, s);

        var innerInvoke = Expression.Invoke(Expression.Constant(inner), s);
        var converted = Expression.Convert(innerInvoke, destNullable);

        var body = Expression.Condition(
            isNullOrWhiteSpace,
            Expression.Default(destNullable),
            converted);

        var lambdaType = typeof(Func<,>).MakeGenericType(typeof(string), destNullable);
        return Expression.Lambda(lambdaType, body, s).Compile();
    }
}
