#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Sql
{
    public abstract class SqlFormatter
    {
        public (string? Sql, IDictionary<string, object?>? Parameters, ColumnCollationInfo? ColumnCollationInfo) Format(SqlStatement statement)
        {
            switch (statement)
            {
                case SqlSelectStatement ss:
                    return _Format(ss, new ColumnCollationInfo());
                case SqlInsertStatement ss:
                    return _Format(ss);
                case SqlUpdateStatement ss:
                    return _Format(ss);
                case SqlDeleteStatement ss:
                    return _Format(ss);
                default:
                    throw new NotImplementedException();
            }
        }

        private (string? Sql, IDictionary<string, object?>? Parameters, ColumnCollationInfo? ColumnCollationInfo) _Format(SqlSelectStatement statement, ColumnCollationInfo? columnCollationInfo)
        {
            var sb = new StringBuilder();
            IDictionary<string, object?>? parameters = null;

            if (statement.Source != null && !statement.ColumnSpecs.Any() && statement.Condition == null && !statement.RelationSpecs.Any())
            {
                // passthru
                return _Format(statement.Source, columnCollationInfo);
            }

            var parameterName = statement.ParameterSetInfo.GetDefaultParamName();
            var parameterType = parameterName != null ? statement.ParameterSetInfo.GetTypeByName(parameterName) ?? statement.Type : null;
            if (parameterType == null) { throw new NotImplementedException(); }

            string source;
            string[] columns;

            if (statement.Source == null)
            {
                columns = GetColumnsByAllTypesToString(statement, columnCollationInfo);
                source = parameterType.GetTableName();
            }
            else
            {
                var child = _Format(statement.Source, null);

                columns = GetColumnsByAllTypesToString(statement, columnCollationInfo);
                source = $"({child.Sql})";
            }

            sb.Append($"SELECT {string.Join(", ", columns)}");
            sb.Append($" FROM {source} {parameterName ?? string.Empty}");

            foreach (var relationSpec in statement.RelationSpecs)
            {
                sb.Append($" LEFT JOIN {relationSpec.Right.Type.GetTableName()} {relationSpec.Right.Name} ON {relationSpec.ConditionSql}");
            }
            if (statement.Condition != null)
            {
                sb.Append($" WHERE {FormatElement(statement.Condition, true, true, parameters)}");
            }

            var ret = sb.Length > 0 ? sb.ToString() : source;
            return (ret.Length > 0 ? ret : null, parameters, columnCollationInfo);
        }

        private (string? Sql, IDictionary<string, object?>? Parameters, ColumnCollationInfo? ColumnCollationInfo) _Format(SqlInsertStatement statement)
        {
            var sb = new StringBuilder();
            IDictionary<string, object?>? parameters = null;

            Func<object, object> _f = (y) => (y is string s ? Quote(s) : (y != null ? y : "NULL"));
            if (statement.IsInsert)
            {
                parameters = null;
                var index = 0;
                object[] rows = statement.Values;

                sb.Append($"INSERT INTO {GetTableName(statement)}");
                sb.Append($"  ({string.Join(", ", statement.Columns)})");
                sb.Append($"  {GetInsertIntoOutputString() ?? ""}");
                sb.Append($"  VALUES");
                foreach (var (row, idx) in rows.Select((x, i) => (x, i)))
                {
                    var d = row.GetType().GetValueDictFromColumnNames(statement.Columns, row);
                    var vals = statement.Columns.Select(c => statement.ExtraData != null && statement.ExtraData.ContainsKey(c) ? statement.ExtraData[c] : d[c]);
                    if (parameters != null)
                    {
                        var cols = statement.Columns.Select(c => $"@{c}_{index}");
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
                        sb.Append($" ({string.Join(", ", vals.Select(_f!))})");
                        if (idx < rows.Length - 1)
                            sb.Append(",");
                    }
                }
                // TODO: PKなしのケースに対応できるように
                sb.Append($"; {GetLastInsertedIdString(statement.PrimaryKey ?? "id", rows.Length) ?? ""}");
                // sb.Append($"; SELECT last_insert_rowid() AS new_id;");
            }

            var ret = sb.ToString();
            return (ret.Length > 0 ? ret : null, parameters, null);
        }

        private (string? Sql, IDictionary<string, object?>? Parameters, ColumnCollationInfo? ColumnCollationInfo) _Format(SqlUpdateStatement statement)
        {
            var sb = new StringBuilder();
            IDictionary<string, object?>? parameters = null;

            Func<object?, object> _f = (y) => (y is string s ? Quote(s) : (y != null ? y : "NULL"));

            foreach (var obj in statement.Values)
            {
                var pcontext = PersistableState.Generate(obj);
                var dirtyKeys = pcontext.DirtyKeys;
                if (dirtyKeys != null && dirtyKeys.Length > 0)
                {
                    sb.Append($"UPDATE {GetTableName(statement)} SET ");
                    var d = obj.GetType().GetValueDictFromColumnNames(dirtyKeys.Select(x => x.ToSnake()).ToArray(), obj);
                    var valstr = string.Join(", ", d.Select(x => $"{x.Key} = {_f(x.Value)}"));
                    sb.Append(valstr);
                    sb.Append($" WHERE {statement.PrimaryKey} = {obj.GetType().GetPrimaryValue(obj)};");
                }
            }

            var ret = sb.ToString();
            return (ret.Length > 0 ? ret : null, parameters, null);
        }

        private (string? Sql, IDictionary<string, object?>? Parameters, ColumnCollationInfo? ColumnCollationInfo) _Format(SqlDeleteStatement statement)
        {
            var sb = new StringBuilder();

            IDictionary<string, object?>? parameters = null;

            sb.Append($"DELETE FROM {GetTableName(statement)}");
            sb.Append($" WHERE {FormatElement(statement.Condition!, false, true, parameters)}");

            var ret = sb.ToString();
            return (ret.Length > 0 ? ret : null, parameters, null);
        }

        private string ValueToString(object? value, bool useQuote)
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
                    return value?.ToString() ?? string.Empty;
            }
        }

        protected string FormatElement(Expression exp, bool showsParameterName, bool useQuote, IDictionary<string, object?>? parameters)
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
                                var value = field.GetValue(container);
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

        private string[] GetColumnsByAllTypesToString(SqlSelectStatement statement, ColumnCollationInfo? columnCollationInfo)
        {
            var columnSpecs = statement.ColumnSpecs;
            if (!columnSpecs.Any())
            {
                var parameters = statement.ParameterSetInfo.GetAllParameters();

                return parameters.SelectMany(x =>
                {
                    var (name, type) = x;
                    var ci = type.GetClassInfo();
                    if (ci == null) { throw new InvalidOperationException(); }
                    var colnames = ci.Columns.Select(x => x.Name).ToArray();

                    if (columnCollationInfo != null)
                    {
                        //return colnames.Select(colname => $"{name}.{colname}");
                        return columnCollationInfo.MakeColumnAliasingDictionary(name, colnames).Select(x => $"{x.Key} AS {x.Value}");
                    }
                    else
                    {
                        return colnames.Select(colname => $"{name}.{colname}");
                    }
                }).ToArray();
            }
            else
            {
                if (columnCollationInfo != null)
                {
                    return columnSpecs.Select(x =>
                    {
                        var names = x.Alias != null ? new[] { x.Name, x.Alias } : new[] { x.Name };
                        var dict = columnCollationInfo.MakeColumnAliasingDictionary(x.Parameter, new string[][] { names }).First();
                        return $"{dict.Key} AS {dict.Value}";
                    }).ToArray();
                }
                else
                {
                    return columnSpecs.Select(x =>
                    {
                        return x.Alias != null ? $"{x.Parameter}.{x.Name} AS {x.Alias}" : $"{x.Parameter}.{x.Name}";
                    }).ToArray();
                }
            }
        }

#if false
        private string GetColumnsToString(string name, string[] colnames, ColumnCollationInfo caInfo)
        {
            if (caInfo != null)
            {
                var aliasedDict = caInfo.MakeColumnAliasingDictionary(name, colnames);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }
            else
            {
                return string.Join(", ", colnames.Select(x => $"{name}.{x}"));
            }
        }
#endif

        private string GetTableName(SqlStatement statement)
        {
            // throw new NotImplementedException();
            if (statement is SqlSelectStatement selectStatement)
            {
                if (selectStatement.Source != null)
                {
                    var sourceString = selectStatement.Source.ToString();
                    if (sourceString != null)
                    {
                        return sourceString;
                    }
                }
            }

            return statement.Type.GetTableName();
        }

        private string Quote(string s)
        {
            return $"'{s.Replace(@"'", @"''")}'";
        }

        protected virtual string? GetInsertIntoOutputString() => null;

        protected virtual string? GetLastInsertedIdString(string pkey, int rows) => null;
    }
}
