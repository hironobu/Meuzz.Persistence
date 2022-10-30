#nullable enable

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;

namespace Meuzz.Persistence.Sql
{
    public class SqlStatement
    {
        public SqlStatement(Type t)
        {
            Type = t;
        }

        public Type Type { get; }
    }

    public abstract class SqlConditionalStatement : SqlStatement
    {
        public SqlConditionalStatement(Type t, Expression? condition = null) : base(t)
        {
            Condition = condition;
        }

        public Expression? Condition { get; private set; }

        public virtual void BuildCondition(LambdaExpression cond, Type? t)
        {
            this.Condition = cond;
        }

        public virtual void BuildCondition(string key, params object[] value)
        {
            var t = Type;
            Expression memberAccessor;
            ParameterExpression px;

            // var ppi = string.IsNullOrEmpty(key) ? t?.GetPrimaryPropertyInfo() : t?.GetProperty(key.ToCamel(true));
            var k = string.IsNullOrEmpty(key) ? t?.GetPrimaryKey() : key;
            var ppi = k != null && t != null ? t.GetPropertyInfoFromColumnName(k) : null;
            if (ppi != null)
            {
                px = Expression.Parameter(t, "_t0");
                memberAccessor = Expression.MakeMemberAccess(px, ppi);
            }
            else
            {
                var t1 = typeof(Dictionary<string, object?>);
                px = Expression.Parameter(t1, "_t0");
                memberAccessor = ExpressionHelpers.MakeDictionaryAccessorExpression(px, key);
            }

            Expression f;

            if (value.Length == 1)
            {
                f = Expression.Equal(
                    Expression.Convert(memberAccessor, value[0].GetType()),
                    Expression.Constant(value[0])
                    );
            }
            else
            {
                var ff = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static).Where(x => x.Name == "Contains" && x.GetParameters().Count() == 2).Single()
                    .MakeGenericMethod(typeof(object));
                f = Expression.Call(ff,
                    Expression.Constant(value),
                    Expression.Convert(memberAccessor, typeof(object))
                    );
            }

            BuildCondition(Expression.Lambda(f, px), t);
        }
    }
}
