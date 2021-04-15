using Meuzz.Foundation;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Meuzz.Persistence.Sql
{
    public abstract class SqlFormatter
    {

        public (string Sql, IDictionary<string, object> Parameters) Format(SqlStatement statement, SqlConnectionContext context)
        {
            var sb = new StringBuilder();
            if (context != null)
            {
                context.ColumnCollationInfo = new ColumnCollationInfo();
            }

            IDictionary<string, object> parameters = null;

            switch (statement)
            {
                case SqlSelectStatement selectStatement:
                    var parameterName = selectStatement.ParamInfo.GetDefaultParamName();
                    var parameterType = selectStatement.ParamInfo.GetParameterTypeByParamName(parameterName);

                    sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.ParamInfo.GetAllParameters(), context != null ? context.ColumnCollationInfo : null))}");

                    sb.Append($" FROM {parameterType.GetTableName()} {parameterName}");

                    foreach (var bindingSpec in selectStatement.GetAllBindings())
                    {
                        sb.Append($" LEFT JOIN {bindingSpec.Foreign.Type.GetTableName()} {bindingSpec.Foreign.Name} ON {bindingSpec.ConditionSql}");
                    }
                    sb.Append($" WHERE {FormatElement(selectStatement.Condition, true, true, parameters)}");
                    break;

                case SqlInsertOrUpdateStatement insertOrUpdateStatement:
                    Func<object, object> _f = (y) => (y is string s ? Quote(s) : (y != null ? y : "NULL"));
                    if (insertOrUpdateStatement.IsInsert)
                    {
                        parameters = null;
                        var index = 0;
                        object[] rows = insertOrUpdateStatement.Values;

                        sb.Append($"INSERT INTO {insertOrUpdateStatement.TableName}");
                        sb.Append($"  ({string.Join(", ", insertOrUpdateStatement.Columns)})");
                        sb.Append($"  {GetInsertIntoOutputString() ?? ""}");
                        sb.Append($"  VALUES");
                        foreach (var (row, idx) in rows.Select((x, i) => (x, i)))
                        {
                            var d = row.GetType().GetValueDictFromColumnNames(insertOrUpdateStatement.Columns, row);
                            var vals = insertOrUpdateStatement.Columns.Select(c => insertOrUpdateStatement.ExtraData != null && insertOrUpdateStatement.ExtraData.ContainsKey(c) ? insertOrUpdateStatement.ExtraData[c] : d[c]);
                            if (parameters != null)
                            {
                                var cols = insertOrUpdateStatement.Columns.Select(c => $"@{c}_{index}");
                                sb.Append($" ({string.Join(", ", cols)})");
                                if (idx < rows.Length - 1)
                                    sb.Append(",");
                                foreach (var (c, v) in cols.Zip(vals))
                                {
                                    parameters.Add(c, v);
                                }
                                index++;
                            }
                            else
                            {
                                sb.Append($" ({string.Join(", ", vals.Select(_f))})");
                                if (idx < rows.Length - 1)
                                    sb.Append(",");
                            }
                        }
                        sb.Append($"; {GetLastInsertedIdString(insertOrUpdateStatement.PrimaryKey, rows.Length) ?? ""}");
                        // sb.Append($"; SELECT last_insert_rowid() AS new_id;");
                    }
                    else
                    {
                        foreach (var obj in insertOrUpdateStatement.Values)
                        {
                            /* var d = obj.GetType().GetValueDictFromColumnNames(insertOrUpdateStatement.Columns, obj); */
                            var pcontext = PersistenceContext.Generate(obj);
                            var dirtyKeys = pcontext.DirtyKeys;
                            if (dirtyKeys != null && dirtyKeys.Length > 0)
                            {
                                sb.Append($"UPDATE {insertOrUpdateStatement.TableName} SET ");
                                var d = obj.GetType().GetValueDictFromColumnNames(dirtyKeys.Select(x => StringUtils.ToSnake(x)).ToArray(), obj);
                                var valstr = string.Join(", ", d.Select(x => $"{x.Key} = {_f(x.Value)}"));
                                sb.Append(valstr);
                                sb.Append($" WHERE {insertOrUpdateStatement.PrimaryKey} = {obj.GetType().GetPrimaryValue(obj)};");
                            }
                        }
                    }
                    break;

                case SqlDeleteStatement deleteStatement:
                    sb.Append($"DELETE FROM {deleteStatement.TableName}");
                    sb.Append($" WHERE {FormatElement(deleteStatement.Condition, false, true, parameters)}");
                    break;

                default:
                    throw new NotImplementedException();
            }

            var ret = sb.ToString();
            return (ret.Length > 0 ? ret : null, parameters);
        }

        private string ValueToString(object value, bool useQuote)
        {
            switch (value)
            {
                case string s:
                    return useQuote ? Quote(s) : s;

                case int[] ns:
                    return string.Join(", ", ns);

                case object[] objs:
                    return string.Join(", ", objs.Select(x => ValueToString(x, useQuote)));

                default:
                    return value.ToString();
            }
        }

        protected string FormatElement(Expression exp, bool showsParameterName, bool useQuote, IDictionary<string, object> parameters)
        {
            switch (exp)
            {
                case LambdaExpression lmbe:
                    return FormatElement(lmbe.Body, showsParameterName, useQuote, parameters);

                case BinaryExpression bine:
                    switch (bine.NodeType)
                    {
                        case ExpressionType.AndAlso:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) AND ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.Or:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) OR ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.LessThan:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) < ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.LessThanOrEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) <= ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.GreaterThan:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) > ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.GreaterThanOrEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) >= ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.Equal:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) = ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";
                        case ExpressionType.NotEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, useQuote, parameters)}) != ({FormatElement(bine.Right, showsParameterName, useQuote, parameters)})";

                        // case ExpressionType.MemberAccess:
                        // return $"{FormatElement(bine.Left)}.{FormatElement(bine.Right)}";
                        default:
                            throw new NotImplementedException();
                    }

                case ConstantExpression ce:
                    return ValueToString(ce.Value, useQuote);

                case ParameterExpression pe:
                    return showsParameterName ? pe.Name : "";

                case MemberExpression me:
                    if (me.Expression.NodeType == ExpressionType.Parameter && !showsParameterName)
                    {
                        return $"{(me.Member.Name)}";
                    }
                    else if (me.Expression.NodeType == ExpressionType.Constant)
                    {
                        var container = ((ConstantExpression)me.Expression).Value;
                        var member = me.Member;
                        switch (member)
                        {
                            case FieldInfo field:
                                object value = field.GetValue(container);
                                if (parameters != null)
                                {
                                    switch (value)
                                    {
                                        case int[] ns:
                                            {
                                                var ks = new List<string>();
                                                foreach (var (n, i) in ns.Select((x, i) => (x, i)))
                                                {
                                                    var k = $"@{field.Name}_{i}";
                                                    ks.Add(k);
                                                    parameters.Add(k, n);
                                                }
                                                return string.Join(", ", ks);
                                            }

                                        case object[] objs:
                                            {
                                                var ks = new List<string>();
                                                foreach (var (o, i) in objs.Select((x, i) => (x, i)))
                                                {
                                                    var k = $"@{field.Name}_{i}";
                                                    ks.Add(k);
                                                    parameters.Add(k, o);
                                                }
                                                return string.Join(", ", ks);
                                            }

                                        default:
                                            {
                                                var k = $"@{field.Name}";
                                                parameters.Add(k, value);
                                                return k;
                                            }
                                    }
                                }
                                else
                                {
                                    return ValueToString(value, useQuote);
                                }
                        }
                        throw new NotImplementedException();
                    }
                    else
                    {
                        return $"{FormatElement(me.Expression, showsParameterName, useQuote, parameters)}.{(me.Member.Name)}";
                    }

                case MethodCallExpression mce:
                    switch (mce.Method.Name)
                    {
                        case "Contains":
                            return $"({FormatElement(mce.Arguments[1], showsParameterName, useQuote, parameters)}) IN ({FormatElement(mce.Arguments[0], showsParameterName, useQuote, parameters)})";

                        case "get_Item":
                            return $"{FormatElement(mce.Object, showsParameterName, useQuote, parameters)}.{FormatElement(mce.Arguments[0], showsParameterName, false, parameters)}";
                    }
                    break;

                case UnaryExpression ue:
                    switch (ue.NodeType)
                    {
                        case ExpressionType.Convert:
                            return $"{FormatElement(ue.Operand, showsParameterName, useQuote, parameters)}";
                    }
                    break;
            }

            throw new NotImplementedException();
        }

        private string[] GetColumnsToString((string, Type)[] pes, ColumnCollationInfo caInfo)
        {
            return pes.Select(x =>
            {
                var (name, type) = x;
                var colnames = type.GetClassInfo().Columns.Select(x => x.Name);
                if (caInfo != null)
                {
                    var aliasedDict = caInfo.MakeColumnAliasingDictionary(name, colnames);
                    return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
                }
                else
                {
                    return string.Join(", ", colnames.Select(x => $"{name}.{x}"));
                }
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s.Replace(@"'", @"''")}'";
        }

        protected virtual string GetInsertIntoOutputString() => null;

        protected virtual string GetLastInsertedIdString(string pkey, int rows) => null;
    }

    public class SqliteFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT (last_insert_rowid() - {rows - 1}) AS {pkey};";
        }
    }

    public class MssqlFormatter : SqlFormatter
    {
        protected override string GetInsertIntoOutputString() => $"OUTPUT INSERTED.ID";
    }

    public class MySqlFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT last_insert_id() AS {pkey};";
        }
    }

}
