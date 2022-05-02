#nullable enable

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Meuzz.Persistence.Core
{
    internal static class ExpressionHelpers
    {
        public static (ParameterExpression, MethodCallExpression) MakeDictionaryAccessorExpression(string key)
        {
            var t1 = typeof(IDictionary<string, object?>);
            var px = Expression.Parameter(t1, "x");
            var methodInfo = t1.GetMethod("get_Item");
            return (px, Expression.Call(px, methodInfo, Expression.Constant(key)));
        }
    }
}
