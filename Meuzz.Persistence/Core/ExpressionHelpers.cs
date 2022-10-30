#nullable enable

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
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
}
