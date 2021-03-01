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
        public abstract string Format(SqlStatement statement);
    }

    public class SqliteFormatter : SqlFormatter
    {
        public SqliteFormatter()
        {
        }

        private TableRef GetTableRef(SqlParameterElement pe, TypeInfo typeInfo)
        {
            var table = typeInfo.GetTableNameFromClassName(pe.Type);
            var paramname = pe.ParamKey.ToLower();

            return new TableRef() { Name = table, Parameter = paramname };
        }

        public override string Format(SqlStatement statement)
        {
            var sb = new StringBuilder();

            if (statement is SqlSelectStatement selectStatement)
            {
                var parameter = selectStatement.Parameters.First();

                sb.Append($"SELECT {string.Join(", ", GetColumnsToString(selectStatement.Parameters.ToArray(), statement.TypeInfo, selectStatement.ColumnAliasingInfo))}");

                sb.Append($" FROM {GetTableRef(parameter, statement.TypeInfo)}");

                foreach (var je in selectStatement.Relations)
                {
                    var pe = je.Right as SqlParameterElement;
                    sb.Append($" LEFT JOIN {GetTableRef(pe, statement.TypeInfo)} ON {GetTableRef(parameter, statement.TypeInfo).Parameter}.{je.PrimaryKey ?? statement.TypeInfo.GetPrimaryKey(parameter.Type)} = {GetTableRef(pe, statement.TypeInfo).Parameter}.{je.ForeignKey}");
                }
                sb.Append($" WHERE {FormatElement(selectStatement.Conditions, statement.TypeInfo)}");

            }
            return sb.ToString();
        }


        protected string FormatElement(SqlElement element, TypeInfo typeInfo)
        {
            if (element is SqlBinaryElement bine)
            {
                switch (bine.Verb)
                {
                    case SqlElementVerbs.And:
                        return $"({FormatElement(bine.Left, typeInfo)}) AND ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Or:
                        return $"({FormatElement(bine.Left, typeInfo)}) OR ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Lt:
                        return $"({FormatElement(bine.Left, typeInfo)}) < ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Lte:
                        return $"({FormatElement(bine.Left, typeInfo)}) <= ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Gt:
                        return $"({FormatElement(bine.Left, typeInfo)}) > ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Gte:
                        return $"({FormatElement(bine.Left, typeInfo)}) >= ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Eq:
                        return $"({FormatElement(bine.Left, typeInfo)}) = ({FormatElement(bine.Right, typeInfo)})";
                    case SqlElementVerbs.Ne:
                        return $"({FormatElement(bine.Left, typeInfo)}) != ({FormatElement(bine.Right, typeInfo)})";

                    case SqlElementVerbs.MemberAccess:
                        return $"{FormatElement(bine.Left, typeInfo)}.{FormatElement(bine.Right, typeInfo)}";

                    case SqlElementVerbs.Lambda:
                        // return $"{FormatElementToString(bine.Left)}";
                        return $"{FormatElement(bine.Left, typeInfo)}";

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
                return pe.ParamKey;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private string[] GetColumnsToString(SqlParameterElement[] pes, TypeInfo typeInfo, ColumnAliasingInfo caInfo)
        {
            return pes.Select(x =>
            {
                var props = typeInfo.GetColumnsFromType(x.Type);
                var aliasedDict = caInfo.MakeColumnAliasingDictionary(x.ParamKey, props);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s}'";
        }


        public class TableRef
        {
            public string Name;
            public string Parameter;

            public override string ToString()
            {
                return $"{Name} {Parameter}";
            }

            public static implicit operator string(TableRef tableRef)
            {
                return tableRef.ToString();
            }
        }

    }
}
