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
        public abstract string Format(SqlStatement statement, out SqlConnectionContext context);
    }

    public class SqliteFormatter : SqlFormatter
    {
        public SqliteFormatter()
        {
        }

        public override string Format(SqlStatement statement, out SqlConnectionContext context)
        {
            var sb = new StringBuilder();
            var sqliteContext = new SqliteConnectionContext();

            switch (statement)
            {
                case SqlSelectStatement selectStatement:
                    var parameterName = selectStatement.ParamInfo.GetDefaultParamName();
                    var parameterType = selectStatement.ParamInfo.GetParameterTypeByParamName(parameterName);

                    sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.ParamInfo.GetAllParameters(), sqliteContext.ColumnAliasingInfo))}");

                    sb.Append($" FROM {parameterType.GetTableName()} {parameterName}");

                    foreach (var bindingSpec in selectStatement.GetAllBindings())
                    {
                        /*var cond = (bindingSpec.ConditionExpression != null
                            ? FormatElement(bindingSpec.ConditionExpression)
                            : $"{bindingSpec.PrimaryParamName}.{bindingSpec.PrimaryKey ?? bindingSpec.PrimaryType.GetPrimaryKey()} {"="} {bindingSpec.ForeignParamName}.{bindingSpec.ForeignKey}"
                            );*/
                        var cond = bindingSpec.ConditionSql;
                        sb.Append($" LEFT JOIN {bindingSpec.ForeignType.GetTableName()} {bindingSpec.ForeignParamName} ON {cond}");
                    }
                    sb.Append($" WHERE {FormatElement(selectStatement.Condition)}");
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
            }

            context = sqliteContext;
            return sb.ToString();
        }


        protected string FormatElement(Expression exp)
        {
            switch (exp)
            {
                case LambdaExpression lmbe:
                    return FormatElement(lmbe.Body);

                case BinaryExpression bine:
                    switch (bine.NodeType)
                    {
                        case ExpressionType.AndAlso:
                            return $"({FormatElement(bine.Left)}) AND ({FormatElement(bine.Right)})";
                        case ExpressionType.Or:
                            return $"({FormatElement(bine.Left)}) OR ({FormatElement(bine.Right)})";
                        case ExpressionType.LessThan:
                            return $"({FormatElement(bine.Left)}) < ({FormatElement(bine.Right)})";
                        case ExpressionType.LessThanOrEqual:
                            return $"({FormatElement(bine.Left)}) <= ({FormatElement(bine.Right)})";
                        case ExpressionType.GreaterThan:
                            return $"({FormatElement(bine.Left)}) > ({FormatElement(bine.Right)})";
                        case ExpressionType.GreaterThanOrEqual:
                            return $"({FormatElement(bine.Left)}) >= ({FormatElement(bine.Right)})";
                        case ExpressionType.Equal:
                            return $"({FormatElement(bine.Left)}) = ({FormatElement(bine.Right)})";
                        case ExpressionType.NotEqual:
                            return $"({FormatElement(bine.Left)}) != ({FormatElement(bine.Right)})";

                        case ExpressionType.MemberAccess:
                            return $"{FormatElement(bine.Left)}.{FormatElement(bine.Right)}";
                    }
                    break;

                case ConstantExpression ce:
                    return ce.Value is string ? Quote(ce.Value.ToString()) : ce.Value.ToString();

                case ParameterExpression pe:
                    return pe.Name;

                case MemberExpression me:
                    return $"{FormatElement(me.Expression)}.{(me.Member.Name)}";
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
