#nullable enable

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
    internal static class ParameterReplacer
    {
        public static Expression Replace
                        (Expression expression,
                        ParameterExpression source,
                        Expression target)
        {
            return new ParameterReplacerVisitor(source, target).Visit(expression);
        }

        private class ParameterReplacerVisitor : ExpressionVisitor
        {
            public ParameterReplacerVisitor(ParameterExpression source, Expression target)
            {
                _source = source;
                _target = target;
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                var parameters = node.Parameters.Select(p => (ParameterExpression)Visit(p));
                return Expression.Lambda(Visit(node.Body), parameters);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                return node == _source ? _target : base.VisitParameter(node);
            }

            private ParameterExpression _source;
            private Expression _target;
        }
    }

    internal static class ExpressionHelpers
    {
        public static Expression MakeConstantExpression(Type t, object? value)
        {
            switch (t)
            {
                case Type intType when intType == typeof(long):
                    return Expression.Constant(Convert.ToInt64(value));

                case Type intType when intType == typeof(int):
                    return Expression.Constant(Convert.ToInt32(value));

                default:
                    return Expression.Constant(value);
            }
        }

        [Obsolete]
        public static MethodCallExpression MakeDictionaryAccessorExpression(string paramname, string key, ParameterExpression[] parameters)
        {
            var px = parameters.First(x => x.Name == paramname);
            var methodInfo = px.Type.GetMethod("get_Item");
            return Expression.Call(px, methodInfo, Expression.Constant(key));
        }

        public static Expression MakeUnboxExpression(Expression e, Type t)
        {
            var methodInfoToInt64 = typeof(Convert).GetMethod("ToInt64", new[] { typeof(object) });
            var methodInfoToInt32 = typeof(Convert).GetMethod("ToInt32", new[] { typeof(object) });

            return Expression.Convert(t switch
            {
                Type intType when intType == typeof(long) => Expression.Call(methodInfoToInt64, e),
                Type intType when intType == typeof(int) => Expression.Call(methodInfoToInt32, e),
                _ => e,
            }, t);
        }

        public static MethodCallExpression MakeDictionaryAccessorExpression(ParameterExpression pe, string key)
        {
            var methodInfo = pe.Type.GetMethod("get_Item");
            return Expression.Call(pe, methodInfo, Expression.Constant(key));
        }

        public static LambdaExpression MakeUntupleByLastMemberAccessFunc(Type tupleType)
        {
            var pt = Expression.Parameter(tupleType);

            return Expression.Lambda(
                Expression.MakeMemberAccess(pt, tupleType.GetMembers().Last()),
                pt);
        }

        public static LambdaExpression MakeEqualityConditionFunc(MemberInfo left, MemberInfo right)
        {
            var px = Expression.Parameter(left.DeclaringType);
            var py = Expression.Parameter(right.DeclaringType);
            
            return Expression.Lambda(
                Expression.Equal(
                    Expression.MakeMemberAccess(px, left),
                    Expression.MakeMemberAccess(py, right)),
                px, py);
        }
    }

    public class ExpressionComparer
    {
        public ExpressionComparer(Expression expr)
        {
            Expression = expr;
        }

        public Expression Expression { get; }

        public override bool Equals(object? obj)
        {
            if (Object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (!(obj is ExpressionComparer comparer))
            {
                return false;
            }

            return ExpressionEquals(Expression, comparer.Expression);
        }

        private static bool ExpressionEquals(Expression e1, Expression e2)
        {
            if (e1.NodeType != e2.NodeType)
            {
                return false;
            }

            switch (e1)
            {
                case MemberExpression me1:
                    var me2 = (MemberExpression)e2;
                    return ExpressionEquals(me1.Expression, me2.Expression) && me1.Member == me2.Member && me1.Type == me2.Type;

                case ParameterExpression pe1:
                    var pe2 = (ParameterExpression)e2;
                    return pe1.Name == pe2.Name && pe1.Type == pe2.Type;

                default:
                    throw new NotImplementedException();
            }
        }


        public override int GetHashCode()
        {
            return GetHashCode(Expression);
        }

        private int GetHashCode(MemberInfo memberInfo)
        {
            return memberInfo.GetHashCode();
        }

        private int GetHashCode(Expression expr)
        {
            switch (Expression)
            {
                case MemberExpression me:
                    int h = me.Member.GetHashCode();
                    Console.WriteLine(h);
                    return GetHashCode(me.Member) + GetHashCode(me.Expression);

                case ParameterExpression pe:
                    return pe.Name.GetHashCode() + pe.Type.GetHashCode();

                default:
                    throw new NotImplementedException();
            }
        }

        public static bool operator ==(ExpressionComparer? c1, ExpressionComparer? c2)
        {
            return c1?.Equals(c2) == true;
        }

        public static bool operator !=(ExpressionComparer? c1, ExpressionComparer? c2)
        {
            return !(c1 == c2);
        }

        public static implicit operator ExpressionComparer(Expression expr)
        {
            return new ExpressionComparer(expr);
        }

        public static implicit operator Expression(ExpressionComparer comparer)
        {
            return comparer.Expression;
        }
    }
}
