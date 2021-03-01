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

        private T PickupElement<T>(SqlElement element, Func<SqlElement, bool> matcher, Func<SqlElement, SqlElement> nextElement) where T : SqlElement
        {
            var current = element;

            while (current != null)
            {
                if (matcher(current))
                {
                    return current as T;
                }

                current = nextElement(current);
            }

            return null;
        }

        public override string Format(SqlStatement statement)
        {
            var element = statement.Root;
            var selectElement = PickupElement<SqlBinaryElement>(element, x => x is SqlBinaryElement && (x as SqlBinaryElement).Verb == SqlElementVerbs.Lambda, x => (x as SqlBinaryElement).Left);
            var parameter = selectElement.Right as SqlParameterElement;
            var parameters = new List<SqlParameterElement>() { };
            var conditions = selectElement.Left;

            parameter.ParamKey = statement.TypeInfo.RegisterParameter(parameter.ParamKey, parameter.Type, null);

            var joinings = new List<SqlJoinElement>();
            var current = element;
            while (current != null)
            {
                current = PickupElement<SqlJoinElement>(current, x => x is SqlJoinElement, x => (x as SqlJoinElement)?.Left);
                if (current is SqlJoinElement picked)
                {
                    joinings.Add(picked);
                    parameters.Add(picked.Right as SqlParameterElement);
                    current = picked.Left;

                    var px = (picked.Right as SqlParameterElement);
                    px.ParamKey = statement.TypeInfo.RegisterParameter(px.ParamKey, px.Type, picked.MemberInfo);
                }
            }
            joinings.Reverse();
            parameters.Reverse();
            parameters.Insert(0, parameter);

            var sb = new StringBuilder($"SELECT {string.Join(", ", GetColumnsToString(parameters.ToArray(), statement.TypeInfo))}");

            sb.Append($" FROM {GetTable(parameters.First())}");

            foreach (var je in joinings)
            {
                sb.Append($" LEFT JOIN {GetTable(je.Right as SqlParameterElement)} ON {GetTable(parameter).Parameter}.{je.PrimaryKey ?? statement.TypeInfo.GetPrimaryKey(parameter.Type)} = {GetTable(je.Right as SqlParameterElement).Parameter}.{je.ForeignKey}");
                statement.TypeInfo.SetBindingByKey(GetTable(je.Right as SqlParameterElement).Parameter, je.ForeignKey, je.PrimaryKey ?? statement.TypeInfo.GetPrimaryKey(parameter.Type));
            }
            sb.Append($" WHERE {FormatElement(conditions, statement.TypeInfo)}");

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

        [Obsolete("NOT TO BE HERE")]
        private string GetTableNameFromClassName(Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return StringUtils.Camel2Snake(t.Name);
            }
            return attr.TableName;
        }

        private TableReference GetTable(SqlParameterElement pe)
        {
            var table = GetTableNameFromClassName(pe.Type);
            var paramname = pe.ParamKey.ToLower();

            return new TableReference() { Name = table, Parameter = paramname };
        }

        private string[] GetColumnsToString(SqlParameterElement[] pes, TypeInfo typeInfo)
        {
            return pes.Select(x =>
            {
                var props = typeInfo.GetColumnsFromType(x.Type);
                var aliasedDict = typeInfo.MakeColumnAliasingDictionary(x.ParamKey, props);
                return string.Join(", ", aliasedDict.Select(x => $"{x.Value} AS {x.Key}"));
            }).ToArray();
        }

        private string Quote(string s)
        {
            return $"'{s}'";
        }


        class TableReference
        {
            public string Name;
            public string Parameter;

            public override string ToString()
            {
                return $"{Name} {Parameter}";
            }

            public static implicit operator string(TableReference tableRef)
            {
                return tableRef.ToString();
            }
        }
    }
}
