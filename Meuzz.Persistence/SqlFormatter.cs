using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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

            if (statement is SqlSelectStatement selectStatement)
            {
                var parameter = selectStatement.Parameters.First();

                sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.Parameters.ToArray(), sqliteContext.ColumnAliasingInfo))}");

                sb.Append($" FROM {parameter.Type.GetTableNameFromClassName()} {parameter.Name}");

                foreach (var je in selectStatement.Relations)
                {
                    var jpe = je.Right as SqlParameterElement;
                    // sb.Append($" LEFT JOIN {pe.Type.GetTableNameFromClassName()} {pe.Name} ON {parameter.Name}.{je.PrimaryKey ?? parameter.Type.GetPrimaryKey()} = {pe.Name}.{je.ForeignKey}");
                    sb.Append($" LEFT JOIN {jpe.Type.GetTableNameFromClassName()} {jpe.Name} ON {MakeJoiningCondition(parameter, je)}");
                }
                sb.Append($" WHERE {FormatElement(selectStatement.Conditions)}");

            }

            context = sqliteContext;
            return sb.ToString();
        }

        private string MakeJoiningCondition(SqlParameterElement parameter, SqlJoinElement je)
        {
            var pe = je.Right as SqlParameterElement;
            return $"{parameter.Name}.{je.PrimaryKey ?? parameter.Type.GetPrimaryKey()} = {pe.Name}.{je.ForeignKey}";
        }


        protected string FormatElement(SqlElement element)
        {
            if (element is SqlBinaryElement bine)
            {
                switch (bine.Verb)
                {
                    case SqlElementVerbs.And:
                        return $"({FormatElement(bine.Left)}) AND ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Or:
                        return $"({FormatElement(bine.Left)}) OR ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Lt:
                        return $"({FormatElement(bine.Left)}) < ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Lte:
                        return $"({FormatElement(bine.Left)}) <= ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Gt:
                        return $"({FormatElement(bine.Left)}) > ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Gte:
                        return $"({FormatElement(bine.Left)}) >= ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Eq:
                        return $"({FormatElement(bine.Left)}) = ({FormatElement(bine.Right)})";
                    case SqlElementVerbs.Ne:
                        return $"({FormatElement(bine.Left)}) != ({FormatElement(bine.Right)})";

                    case SqlElementVerbs.MemberAccess:
                        return $"{FormatElement(bine.Left)}.{FormatElement(bine.Right)}";

                    case SqlElementVerbs.Lambda:
                        // return $"{FormatElementToString(bine.Left)}";
                        return $"{FormatElement(bine.Left)}";

                    default:
                        throw new NotImplementedException();
                }
            }
            else if (element is SqlConstantElement ce)
            {
                return ce.Value is string ? Quote(ce.Value.ToString()) : ce.Value.ToString();
            }
            else if (element is SqlLeafElement le)
            {
                return le.Value.ToString();
            }
            else if (element is SqlParameterElement pe)
            {
                return pe.Name;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private string[] GetColumnsToString(SqlParameterElement[] pes, ColumnAliasingInfo caInfo)
        {
            return pes.Select(x =>
            {
                var props = x.Type.GetColumnsFromType();
                var aliasedDict = caInfo.MakeColumnAliasingDictionary(x.Name, props);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s}'";
        }
    }
}
