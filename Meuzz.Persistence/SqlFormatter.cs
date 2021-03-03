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
                var parameterName = selectStatement.ParamInfo.GetDefaultParamName();
                var parameterType = selectStatement.ParamInfo.GetParameterTypeByParamName(parameterName);

                sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.ParamInfo.GetAllParameters(), sqliteContext.ColumnAliasingInfo))}");

                sb.Append($" FROM {parameterType.GetTableName()} {parameterName}");

                foreach (var bindingSpec in selectStatement.GetAllBindings())
                {
                    var cond = $"{bindingSpec.PrimaryParamName}.{bindingSpec.PrimaryKey ?? bindingSpec.PrimaryType.GetPrimaryKey()} {bindingSpec.Comparator} {bindingSpec.ForeignParamName}.{bindingSpec.ForeignKey}";
                    sb.Append($" LEFT JOIN {bindingSpec.ForeignType.GetTableName()} {bindingSpec.ForeignParamName} ON {cond}");
                }
                sb.Append($" WHERE {FormatElement(selectStatement.Root)}");

            }

            context = sqliteContext;
            return sb.ToString();
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

        private string[] GetColumnsToString((string, Type)[] pes, ColumnAliasingInfo caInfo)
        {
            return pes.Select(x =>
            {
                var (name, type) = x;
                var cols = type.GetTableInfoFromType().Select(x => x.ColumnName);
                var aliasedDict = caInfo.MakeColumnAliasingDictionary(name, cols);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s}'";
        }
    }
}
