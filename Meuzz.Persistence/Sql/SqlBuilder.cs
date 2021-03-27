using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence.Sql
{
    public abstract class SqlBuilderBase
    {/*
        public virtual SqlElement BuildCondition(SqlElement parent, Expression expression)
        {
            switch (expression)
            {
                case BinaryExpression be:
                    var lstr = BuildCondition(parent, be.Left);
                    var rstr = BuildCondition(parent, be.Right);
                    return MakeSqlElement(be.NodeType, lstr, rstr);

                case ParameterExpression pe:
                    return MakeSqlElement(pe);

                case MemberExpression me:
                    return MakeSqlElement(me.NodeType, BuildCondition(parent, me.Expression), MakeSqlElement(me));

                case ConstantExpression ce:
                    return MakeSqlElement(ce);
            }

            throw new NotImplementedException();
        }

        protected SqlElement MakeSqlElement(Expression exp)
        {
            switch (exp)
            {
                case ParameterExpression pe:
                    return new SqlParameterElement(pe.Type, pe.Name);

                case ConstantExpression ce:
                    return new SqlConstantElement(ce.Value);

                case MemberExpression me:
                    return new SqlLeafElement(me.Member.GetColumnName());
            }

            throw new NotImplementedException();
        }

        protected SqlElement MakeSqlElement(ExpressionType type, SqlElement x, SqlElement y)
        {
            switch (type)
            {
                case ExpressionType.AndAlso:
                    return new SqlBinaryElement(SqlElementVerbs.And, x, y);

                case ExpressionType.NotEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Ne, x, y);

                case ExpressionType.Equal:
                    return new SqlBinaryElement(SqlElementVerbs.Eq, x, y);

                case ExpressionType.GreaterThan:
                    return new SqlBinaryElement(SqlElementVerbs.Gt, x, y);

                case ExpressionType.GreaterThanOrEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Gte, x, y);

                case ExpressionType.LessThan:
                    return new SqlBinaryElement(SqlElementVerbs.Lt, x, y);

                case ExpressionType.LessThanOrEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Lte, x, y);

                case ExpressionType.MemberAccess:
                    return new SqlBinaryElement(SqlElementVerbs.MemberAccess, x, y);

                case ExpressionType.Lambda:
                    return new SqlBinaryElement(SqlElementVerbs.Lambda, x, y);
            }

            throw new NotImplementedException();
        }*/
    }

    public abstract class SqlBuilder<T> : SqlBuilderBase where T : class, new()
    {
        // public abstract IFilterable<T> BuildSelect();
    }

    public class SqliteSqlBuilder<T> : SqlBuilder<T> where T : class, new()
    {
        /*public override IFilterable<T> BuildSelect()
        {
            return new SelectStatement<T>();
        }*/
    }
}
