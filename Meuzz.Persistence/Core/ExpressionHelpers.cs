#nullable enable

using System.Linq;
using System.Linq.Expressions;

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
    }
}
