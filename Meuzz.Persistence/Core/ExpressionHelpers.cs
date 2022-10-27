#nullable enable

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
    internal static class ExpressionHelpers
    {
        public static MethodCallExpression MakeDictionaryAccessorExpression(string paramname, string key, ParameterExpression[] parameters)
        {
            var px = parameters.First(x => x.Name == paramname);
            var methodInfo = px.Type.GetMethod("get_Item");
            return Expression.Call(px, methodInfo, Expression.Constant(key));
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
