#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Sql
{
    public class SqlStatement
    {
        public SqlStatement(Type t)
        {
            Type = t;
        }

        public SqlStatement(SqlStatement statement) : this(statement.Type)
        {
        }

        public Type Type { get; }
    }

    public abstract class SqlConditionalStatement : SqlStatement
    {
        public SqlConditionalStatement(Type t, Expression? condition = null) : base(t)
        {
            Condition = condition;

            ParameterSetInfo = new ParameterSetInfo();
            ParameterSetInfo.RegisterParameter(null, t, true);
        }

        public SqlConditionalStatement(SqlConditionalStatement statement) : base(statement)
        {
            Condition = statement.Condition;

            ParameterSetInfo = new ParameterSetInfo(statement.ParameterSetInfo);
        }

        public Expression? Condition { get; private set; }

        [Obsolete]
        public ParameterSetInfo ParameterSetInfo { get; }

        public virtual void BuildCondition(LambdaExpression cond, Type? t)
        {
            var p = cond.Parameters.Single();
            ParameterSetInfo.RegisterParameter(p.Name, t ?? p.Type, true);
            // (return) == p.Name

            this.Condition = cond;
        }

        public virtual void BuildCondition(string key, params object[] value)
        {
            Type? t = ParameterSetInfo.GetDefaultParamType();
            Expression memberAccessor;
            ParameterExpression px;

            var ppi = string.IsNullOrEmpty(key) ? t?.GetPrimaryPropertyInfo() : t?.GetProperty(StringUtils.ToCamel(key, true));
            if (ppi != null)
            {
                px = Expression.Parameter(t, "x");
                memberAccessor = Expression.MakeMemberAccess(px, ppi);
            }
            else
            {
                var t1 = typeof(IDictionary<string, object?>);
                px = Expression.Parameter(t1, "x");
                var methodInfo = t1.GetMethod("get_Item");
                memberAccessor = Expression.Call(px, methodInfo, Expression.Constant(key));
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
