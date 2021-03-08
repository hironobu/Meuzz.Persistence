using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Meuzz.Persistence
{
    public abstract class SqlFormatter
    {
        public abstract (string, IDictionary<string, object>) Format(SqlStatement statement, out SqlConnectionContext context);
    }

    public class SqliteFormatter : SqlFormatter
    {
        public SqliteFormatter()
        {
        }

        public override (string, IDictionary<string, object>) Format(SqlStatement statement, out SqlConnectionContext context)
        {
            var sb = new StringBuilder();
            var sqliteContext = new SqliteConnectionContext();

            var parameters = new Dictionary<string, object>();

            switch (statement)
            {
                case SqlSelectStatement selectStatement:
                    var parameterName = selectStatement.ParamInfo.GetDefaultParamName();
                    var parameterType = selectStatement.ParamInfo.GetParameterTypeByParamName(parameterName);

                    sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.ParamInfo.GetAllParameters(), sqliteContext.ColumnAliasingInfo))}");

                    sb.Append($" FROM {parameterType.GetTableName()} {parameterName}");

                    foreach (var bindingSpec in selectStatement.GetAllBindings())
                    {
                        var cond = bindingSpec.ConditionSql;
                        sb.Append($" LEFT JOIN {bindingSpec.ForeignType.GetTableName()} {bindingSpec.ForeignParamName} ON {cond}");
                    }
                    sb.Append($" WHERE {FormatElement(selectStatement.Condition, true, parameters)}");
                    break;

                case SqlInsertOrUpdateStatement insertOrUpdateStatement:
                    Func<object, object> _f = (y) => (y is string s ? Quote(s) : (y != null ? y : "NULL"));
                    if (insertOrUpdateStatement.IsInsert)
                    {
                        object[] rowss = insertOrUpdateStatement.IsBulk
                            ? new object[] { insertOrUpdateStatement.Values }
                            : insertOrUpdateStatement.Values.Select(x => new object[] { x }).ToArray();
                        foreach (var x in rowss)
                        {
                            object[] rows = (object[])x;
                            sb.Append($"INSERT INTO {insertOrUpdateStatement.TableName}");
                            sb.Append($" ({string.Join(", ", insertOrUpdateStatement.Columns)})");
                            sb.Append($" VALUES");
                            foreach (var (row, idx) in rows.Select((x, i) => (x, i)))
                            {
                                var d = row.GetValueDictFromColumnNames(insertOrUpdateStatement.Columns);
                                var vals = insertOrUpdateStatement.Columns.Select(c => insertOrUpdateStatement.ExtraData != null && insertOrUpdateStatement.ExtraData.ContainsKey(c) ? insertOrUpdateStatement.ExtraData[c] : d[c]);
                                sb.Append($" ({string.Join(", ", vals.Select(_f))})");
                                if (idx < rows.Length - 1)  
                                    sb.Append(",");
                            }
                            sb.Append($"; SELECT last_insert_rowid() AS new_id; ");
                        }
                    }
                    else
                    {
                        foreach (var obj in insertOrUpdateStatement.Values)
                        {
                            sb.Append($"UPDATE {insertOrUpdateStatement.TableName} SET ");
                            var d = obj.GetValueDictFromColumnNames(insertOrUpdateStatement.Columns);
                            var valstr = string.Join(", ", d.Select(x => $"{x.Key} = {_f(x.Value)}"));
                            sb.Append(valstr);
                            sb.Append($" WHERE {insertOrUpdateStatement.PrimaryKey} = {obj.GetType().GetPrimaryValue(obj)};");
                        }
                    }
                    break;

                case SqlDeleteStatement deleteStatement:
                    sb.Append($"DELETE FROM {deleteStatement.TableName}");
                    sb.Append($" WHERE {FormatElement(deleteStatement.Condition, false, parameters)}");
                    break;

                default:
                    throw new NotImplementedException();
            }

            context = sqliteContext;
            return (sb.ToString(), parameters);
        }

        private string ValueToString(object value)
        {
            switch (value)
            {
                case string s:
                    return Quote(s);

                case int[] ns:
                    return string.Join(", ", ns);

                case object[] objs:
                    return string.Join(", ", objs.Select(x => ValueToString(x)));

                default:
                    return value.ToString();
            }
        }

        protected string FormatElement(Expression exp, bool showsParameterName, IDictionary<string, object> parameters)
        {
            switch (exp)
            {
                case LambdaExpression lmbe:
                    return FormatElement(lmbe.Body, showsParameterName, parameters);

                case BinaryExpression bine:
                    switch (bine.NodeType)
                    {
                        case ExpressionType.AndAlso:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) AND ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.Or:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) OR ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.LessThan:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) < ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.LessThanOrEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) <= ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.GreaterThan:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) > ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.GreaterThanOrEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) >= ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.Equal:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) = ({FormatElement(bine.Right, showsParameterName, parameters)})";
                        case ExpressionType.NotEqual:
                            return $"({FormatElement(bine.Left, showsParameterName, parameters)}) != ({FormatElement(bine.Right, showsParameterName, parameters)})";

                        // case ExpressionType.MemberAccess:
                        // return $"{FormatElement(bine.Left)}.{FormatElement(bine.Right)}";
                        default:
                            throw new NotImplementedException();
                    }

                case ConstantExpression ce:
                    return ValueToString(ce.Value);

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
                                    return ValueToString(value);
                                }
                        }
                        throw new NotImplementedException();
                    }
                    else
                    {
                        return $"{FormatElement(me.Expression, showsParameterName, parameters)}.{(me.Member.Name)}";
                    }

                case MethodCallExpression mce:
                    switch (mce.Method.Name)
                    {
                        case "Contains":
                            return $"{FormatElement(mce.Arguments[1], showsParameterName, parameters)} IN ({FormatElement(mce.Arguments[0], showsParameterName, parameters)})";
                    }
                    break;

                case UnaryExpression ue:
                    switch (ue.NodeType)
                    {
                        case ExpressionType.Convert:
                            return $"{FormatElement(ue.Operand, showsParameterName, parameters)}";
                    }
                    break;
            }

            throw new NotImplementedException();
        }

        private string[] GetColumnsToString((string, Type)[] pes, ColumnAliasingInfo caInfo)
        {
            return pes.Select(x =>
            {
                var (name, type) = x;
                var colnames = type.GetTableInfo().Columns.Select(x => x.Name);
                var aliasedDict = caInfo.MakeColumnAliasingDictionary(name, colnames);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s}'";
        }
    }
}
