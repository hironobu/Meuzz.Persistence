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
            TableName = t.GetTableName();
        }

        public SqlStatement(SqlStatement statement)
        {
            TableName = statement.TableName;
        }

        public string TableName { get; }
    }

    public abstract class SqlConditionalStatement : SqlStatement
    {
        public Expression? Condition { get; private set; } = null;

        public ParamInfo ParamInfo { get; } = new ParamInfo();

        public SqlConditionalStatement(Type t) : base(t)
        {
            ParamInfo.RegisterParameter(null, t, true);
        }

        public SqlConditionalStatement(SqlConditionalStatement statement) : base(statement)
        {
            Condition = statement.Condition;
            ParamInfo = statement.ParamInfo;
        }

        public virtual void BuildCondition(LambdaExpression cond, Type? t)
        {
            if (!(cond is LambdaExpression lme))
            {
                throw new NotImplementedException();
            }

            var p = cond.Parameters.Single<ParameterExpression>();
            var defaultParamName = ParamInfo.GetDefaultParamName();
            if (defaultParamName == null)
            {
                ParamInfo.RegisterParameter(p.Name, t ?? p.Type, true);
            }
            else if (defaultParamName != p.Name)
            {
                throw new NotImplementedException();
            }

            this.Condition = cond;
        }

        public virtual void BuildCondition(string key, params object[] value)
        {
            Type? t = ParamInfo.GetDefaultParamType();
            Expression memberAccessor;
            ParameterExpression px;

            var ppi = string.IsNullOrEmpty(key) ? t.GetPrimaryPropertyInfo() : t?.GetProperty(StringUtils.ToCamel(key, true));
            if (ppi != null)
            {
                px = Expression.Parameter(t, "x");
                memberAccessor = Expression.MakeMemberAccess(px, ppi);
            }
            else
            {
                var t1 = typeof(IDictionary<string, object>);
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
