using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public abstract class SqlBuilder<T> where T : class
    {
        public virtual SqlElement BuildElement(SqlStatement statement, SqlElement parent, Expression expression)
        {
            switch (expression)
            {
                case LambdaExpression mce:
                    var p = mce.Parameters.First<ParameterExpression>();
                    var pel = BuildElement(statement, parent, p) as SqlParameterElement;
                    pel.Name = statement.ParamInfo.RegisterParameter(pel.Name, pel.Type, true);
                    return MakeSqlElement(mce.NodeType, BuildElement(statement, parent, mce.Body), pel);

                case BinaryExpression be:
                    var lstr = BuildElement(statement, parent, be.Left);
                    var rstr = BuildElement(statement, parent, be.Right);
                    // return $"[{be.NodeType} <{lstr}> <{rstr}>]";
                    return MakeSqlElement(be.NodeType, lstr, rstr);

                case ParameterExpression pe:
                    // var mstr = GetColumnNameFromProperty(pe.);
                    return MakeSqlElement(pe);

                //case Property pie:
                //    return $"<P <{pie.ToString()}>>"
                case MemberExpression me:
                    return MakeSqlElement(me.NodeType, BuildElement(statement, parent, me.Expression), MakeSqlElement(me));

                case ConstantExpression ce:
                    return MakeSqlElement(ce);

                default:
                    throw new NotImplementedException();
            }
        }

        protected string GetTableNameFromType(Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return StringUtils.ToSnake(t.Name);
            }
            return attr.TableName;
        }


        public abstract SelectStatement<T> BuildSelect(Expression expression, Func<SelectStatement<T>, IEnumerable<T>> f);
        // public abstract SqlStatement BuildSelectAndInclude<T>(Expression expression, Expression expression2);

        public string GetColumnNameFromProperty(MemberInfo mi)
        {
            var attr = mi.GetCustomAttribute<PersistentPropertyAttribute>();
            if (attr == null || attr.Column == null)
            {
                return StringUtils.ToSnake(mi.Name);
            }

            return attr.Column;
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
                    var mstr2 = GetColumnNameFromProperty(me.Member);
                    return new SqlLeafElement(mstr2.ToLower());
                default:
                    throw new NotImplementedException();
            }
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

                default:
                    throw new NotImplementedException();
            }
        }
    }

    public class SqliteSqlBuilder<T> : SqlBuilder<T> where T : class
    {
        private string Table = null;
        public SqliteSqlBuilder()
        {
            Table = GetTableNameFromType(typeof(T));
        }

        public override SelectStatement<T> BuildSelect(Expression expression, Func<SelectStatement<T>, IEnumerable<T>> f)
        {
            var statement = new SelectStatement<T>(null) { OnExecute = f };
            statement.Root = BuildElement(statement, null, expression);
            return statement;
        }
    }
}
