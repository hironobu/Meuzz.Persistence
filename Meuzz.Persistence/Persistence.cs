using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class ParamInfo
    {
        private IDictionary<string, Type> _parameters = new Dictionary<string, Type>();
        private string _defaultParamName = null;

        private IDictionary<string, MemberInfo> _memberInfos = new Dictionary<string, MemberInfo>();

        public void ResetParameters()
        {
            _parameters.Clear();
            _memberInfos.Clear();
        }

        public string RegisterParameter(string key, Type t, MemberInfo memberInfo)
        {
            var k = key;
            int i = 0;
            while (_parameters.ContainsKey(k))
            {
                k = $"{key}{i++}";
            }
            _parameters.Add(k, t);
            if (memberInfo == null)
            {
                _defaultParamName = k;
            }
            else
            {
                _memberInfos.Add(k, memberInfo);
            }

            return k;
        }

        public Type GetParameterTypeByParamName(string name)
        {
            return _parameters[name];
        }


        public MemberInfo GetMemberInfoByParamName(string name)
        {
            return _memberInfos[name];
        }


        public string GetDefaultParamName()
        {
            return _defaultParamName;
        }


        public (string, string, Func<dynamic, dynamic, bool>) GetBindingByParamName(string name)
        {
            return (_bindings[name][0] as string, _bindings[name][1] as string, _bindings[name][2] as Func<dynamic, dynamic, bool>);
        }

        public void SetBindingByKey(string name, string foreignKey, string primaryKey, Func<dynamic, dynamic, bool> condfunc)
        {
            _bindings.Add(name, new object[] { foreignKey, primaryKey, condfunc });
        }

        private IDictionary<string, object[]> _bindings = new Dictionary<string, object[]>();

        public Func<dynamic, Func<dynamic, bool>> GetJoiningConditionByParamName(string name)
        {/*
            var (foreignKey, primaryKey, condfunc) = GetBindingByParamName(name);
            Func<Func<dynamic, dynamic>, Func<dynamic, dynamic>, Func<dynamic, dynamic, bool>, Func<dynamic, Func<dynamic, bool>>> joiningConditionMaker = (Func<dynamic, dynamic> f, Func<dynamic, dynamic> g, Func<dynamic, dynamic, bool> ev) => (dynamic x) => (dynamic y) => ev(f(x), g(y)); // propertyGetter(defaultType, primaryKey)(l) == dictionaryGetter(foreignKey)(r);

            Func<dynamic, dynamic, bool> evaluator = (x, y) =>
            {
                return x == y;
            };
            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.Snake2Camel(prop, true)).GetValue(x);
            Func<string, Func<dynamic, dynamic>> dictionaryGetter = (string key) => (dynamic x) => x[key];

            return joiningConditionMaker(propertyGetter(primaryKey), dictionaryGetter(foreignKey), evaluator);*/

            var (foreignKey, primaryKey, condfunc) = GetBindingByParamName(name);
            return (x) => (y) => condfunc(x, y);
        }
    }


    public class ColumnAliasingInfo
    {
        private IDictionary<string, string> _aliasingProperties = new Dictionary<string, string>();

        public IDictionary<string, string> MakeColumnAliasingDictionary(string paramName, IEnumerable<string> props)
        {
            var propKeys = props.Select(x => $"{paramName}.{x}");
            foreach (var k in propKeys)
            {
                if (!_aliasingProperties.ContainsKey(k))
                {
                    _aliasingProperties.Add($"_c{_aliasingProperties.Count()}", k);
                }
            }

            return _aliasingProperties.Where(x => propKeys.Contains(x.Value)).ToDictionary(x => x.Key, x => x.Value);
        }

        public string GetOriginalColumnName(string c)
        {
            return _aliasingProperties[c];
        }

    }


    public static class TypeInfoExtensions
    {
        public static IEnumerable<string> GetColumnsFromType(this Type t)
        {
            return TypeInfoDict[t].Select(x => x.ColumnName);
        }

        private static IDictionary<Type, ColumnInfoEntry[]> TypeInfoDict { get; set; } = new Dictionary<Type, ColumnInfoEntry[]>();

        public static bool IsPersistent(this Type t)
        {
            return TypeInfoDict.TryGetValue(t, out var _);
        }

        public static void MakeTypePersistent(this Type t, Func<string, string[]> tableInfoGetter)
        {
            if (!t.IsPersistent())
            {
                var colinfos = new List<ColumnInfoEntry>();
                // var rset = _connection.Execute($"PRAGMA table_info('{GetTableNameFromClassName(t)}')");
                var classprops = t.GetProperties().ToList();
                // foreach (var result in rset.Results)
                foreach (var tableName in tableInfoGetter(GetTableNameFromClassName(t)))
                {
                    // var tableName = result["name"].ToString();
                    var prop = t.GetPropertyFromColumnName(tableName);
                    colinfos.Add(new ColumnInfoEntry() { ColumnName = tableName, MemberInfo = prop });
                    classprops.Remove(prop);
                }

                TypeInfoDict[t] = colinfos.ToArray();

                foreach (var prop in t.GetProperties())
                {
                    if (prop.PropertyType.IsGenericType)
                    {
                        prop.PropertyType.GetGenericArguments()[0].MakeTypePersistent(tableInfoGetter);
                    }
                    else
                    {
                        prop.PropertyType.MakeTypePersistent(tableInfoGetter);
                    }
                }

            }
        }

        private static string GetShortColumnName(string fcol)
        {
            return fcol.Split('.').Last();
        }

        public static PropertyInfo GetPropertyFromColumnName(this Type t, string fcol)
        {
            var c = GetShortColumnName(fcol).ToLower();
            foreach (var p in t.GetProperties())
            {
                var cc = StringUtils.ToSnake(p.Name).ToLower();
                var ppa = p.GetCustomAttribute<PersistentPropertyAttribute>();
                if (ppa != null && ppa.Column != null)
                {
                    cc = ppa.Column.ToLower();
                }

                if (cc == c)
                {
                    return p;
                }
            }

            return null;
        }

        public static string GetPrimaryKey(this Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr != null && attr.PrimaryKey != null)
            {
                return attr.PrimaryKey;
            }
            return "id";
        }

        [Obsolete("MIGHT BE HERE")]
        public static string GetTableNameFromClassName(this Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return StringUtils.ToSnake(t.Name);
            }
            return attr.TableName;
        }


        public class ColumnInfoEntry
        {
            public string ColumnName;
            public MemberInfo MemberInfo;
        }
    }


    [AttributeUsage(AttributeTargets.Class)]
    public class PersistentClassAttribute : Attribute
    {
        public string TableName = null;
        public string PrimaryKey = null;

        public PersistentClassAttribute(string tableName, string primaryKey = null)
        {
            this.TableName = tableName;
            this.PrimaryKey = primaryKey;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class PersistentPropertyAttribute : Attribute
    {
        public string Column = null;
        public PersistentPropertyAttribute(string column = null)
        {
            Column = column;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class HasManyAttribute : Attribute
    {
        public Type To = null;
        public string PrimaryKey = null;
        public string ForeignKey = null;

        public HasManyAttribute(Type to, string primaryKey = null, string foreignKey = null)
        {
            this.To = to;
            this.ForeignKey = foreignKey;
        }
    }
}
