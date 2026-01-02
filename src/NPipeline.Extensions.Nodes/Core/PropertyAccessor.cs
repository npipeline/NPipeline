using System.Linq.Expressions;
using System.Reflection;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     Utility for compiling strongly-typed property/field accessors (getter and setter) from expressions,
///     avoiding reflection in hot paths. Supports nested member access (e.g., x => x.Address.Street).
///     Throws at configuration time when a provided selector cannot be assigned to (e.g., read-only property).
/// </summary>
public static class PropertyAccessor
{
    /// <summary>
    ///     Compile getter and setter delegates from a member access expression like x => x.Property or x => x.Field.
    ///     Supports nested paths and validates that the final member is settable.
    /// </summary>
    public static Accessor<T, TProp> Create<T, TProp>(Expression<Func<T, TProp>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        // Compile getter directly
        var getter = selector.Compile();

        // Extract final member access (handle potential Convert/Unary wrappers)
        var body = RemoveUnary(selector.Body);

        if (body is not MemberExpression memberExpression)
        {
            throw new ArgumentException(
                $"Selector must be a simple member access expression. Received: {selector}",
                nameof(selector));
        }

        var memberName = GetMemberPath(memberExpression);

        // Validate assignability and build setter expression
        var targetParam = Expression.Parameter(typeof(T), "target");
        var valueParam = Expression.Parameter(typeof(TProp), "value");

        // Replace the original parameter with targetParam across the entire expression tree
        var replacedMember = (MemberExpression)new ReplaceParameterVisitor(selector.Parameters[0], targetParam).Visit(memberExpression);

        // Ensure last member in the chain is assignable
        if (replacedMember.Member is PropertyInfo pi)
        {
            if (!pi.CanWrite || pi.SetMethod is null)
            {
                throw new ArgumentException(
                    $"Member '{memberName}' is a property but does not have a public setter.",
                    nameof(selector));
            }
        }
        else if (replacedMember.Member is FieldInfo fi)
        {
            if (fi.IsInitOnly)
            {
                throw new ArgumentException(
                    $"Member '{memberName}' is a field but is readonly (init-only).",
                    nameof(selector));
            }
        }
        else
        {
            throw new ArgumentException(
                $"Member '{memberName}' is not a property or field.",
                nameof(selector));
        }

        // Build assignment: (T target, TProp value) => (target.Member) = value
        var assign = Expression.Assign(replacedMember, valueParam);

        Action<T, TProp> setter;

        try
        {
            setter = Expression.Lambda<Action<T, TProp>>(assign, targetParam, valueParam).Compile();
        }
        catch (Exception ex)
        {
            throw new ArgumentException(
                $"Failed to create setter for member '{memberName}'. Ensure the expression resolves to a settable member. " +
                $"Details: {ex.Message}",
                nameof(selector), ex);
        }

        return new Accessor<T, TProp>(memberName, getter, setter);
    }

    private static Expression RemoveUnary(Expression expr)
    {
        return expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : expr;
    }

    private static string GetMemberPath(MemberExpression memberExpression)
    {
        // Build a dotted path A.B.C from nested MemberExpressions
        var segments = new Stack<string>();
        Expression? current = memberExpression;

        while (current is MemberExpression me)
        {
            segments.Push(me.Member.Name);
            current = me.Expression;
        }

        return string.Join('.', segments);
    }

    /// <summary>
    ///     Represents a compiled property or field accessor with getter and setter delegates.
    /// </summary>
    public sealed record Accessor<T, TProp>(string MemberName, Func<T, TProp> Getter, Action<T, TProp> Setter);

    private sealed class ReplaceParameterVisitor(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
        {
            return node == from
                ? to
                : base.VisitParameter(node);
        }
    }
}
