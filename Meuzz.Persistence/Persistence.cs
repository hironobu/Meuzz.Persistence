using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class BindingInfo
    {
        public string ForeignKey;
        public string PrimaryKey;
        public MemberInfo MemberInfo;
        public Func<dynamic, dynamic, bool> Conditions;
    }

    public class ParamInfo
    {
        private IDictionary<string, Type> _parameters = new Dictionary<string, Type>();
        private string _defaultParamName = null;

        // private IDictionary<string, MemberInfo> _memberInfos = new Dictionary<string, MemberInfo>();

        public void ResetParameters()
        {
            _parameters.Clear();
            // _memberInfos.Clear();
        }

        public string RegisterParameter(string name, Type t, bool asDefault)
        {
            var k = name;
            int i = 0;
            while (_parameters.ContainsKey(k))
            {
                k = $"{name}{i++}";
            }
            _parameters.Add(k, t);
            if (asDefault)
            {
                _defaultParamName = k;
            }

            return k;
        }

        public Type GetParameterTypeByParamName(string name)
        {
            return _parameters[name];
        }

        public (string, Type)[] GetAllParameters()
        {
            return _parameters.Select(x => (x.Key, x.Value)).ToArray();
        }


        /*public MemberInfo GetMemberInfoByParamName(string name)
        {
            return _memberInfos[name];
        }*/


        public string GetDefaultParamName()
        {
            return _defaultParamName;
        }


        public BindingInfo GetBindingInfoByName(string from, string to)
        {
            IDictionary<string, BindingInfo> d = _bindings[from];
            if (d == null) { return null; }
            return d[to];
        }

        public void SetBindingByName(string from, string to, string foreignKey, string primaryKey, MemberInfo memberInfo, Func<dynamic, dynamic, bool> condfunc)
        {
            //IDictionary<string, BindingInfo> d = _bindings[from];
            //if (d == null)
            if (!_bindings.TryGetValue(from, out var d))
            {
                d = new Dictionary<string, BindingInfo>();
                // _bindings[from] = d;
                _bindings.Add(from, d);
            }

            d.Add(to, new BindingInfo() { ForeignKey = foreignKey, PrimaryKey = primaryKey, MemberInfo = memberInfo, Conditions = condfunc });
        }

        private IDictionary<string, IDictionary<string, BindingInfo>> _bindings = new Dictionary<string, IDictionary<string, BindingInfo>>();

        public IEnumerable<(string, string, BindingInfo)> GetAllBindings()
        {
            foreach (var (from, d) in _bindings)
            {
                foreach (var (to, bi) in d)
                {
                    yield return (from, to, bi);
                }
            }
        }

        internal IEnumerable<(string, BindingInfo)> GetBindingsForParamName(string x)
        {
            if (!_bindings.ContainsKey(x))
            {
                yield break;
            }

            foreach (var (to, bi) in _bindings[x])
            {
                yield return (to, bi);
            }
        }

#if false
        public (string, string, Func<dynamic, Func<dynamic, bool>>) GetJoiningConditionByParamName(string name)
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

            var (foreignKey, primaryKey, condfunc) = GetBindingInfoByParamName(name);
            return (foreignKey, primaryKey, (x) => (y) => condfunc(x, y));
        }
#endif
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
        public static IEnumerable<ColumnInfoEntry> GetTableInfoFromType(this Type t)
        {
            return TypeInfoDict[t];
        }

        private static IDictionary<Type, ColumnInfoEntry[]> TypeInfoDict { get; set; } = new Dictionary<Type, ColumnInfoEntry[]>();

        public static bool IsPersistent(this Type t)
        {
            return TypeInfoDict.TryGetValue(t, out var _);
        }

        public static void MakeTypePersistent(this Type t, Func<string, string[]> tableInfoGetter, Func<string, IDictionary<string, object>[]> foreignKeyInfoGetter)
        {
            if (!t.IsPersistent())
            {
                var colinfos = new List<ColumnInfoEntry>();
                // var rset = _connection.Execute($"PRAGMA table_info('{GetTableNameFromClassName(t)}')");
                var classprops = t.GetProperties().ToList();
                // foreach (var result in rset.Results)
                var tableName = GetTableName(t);
                // var pkinfos = new List<ColumnInfoEntry>();
                var fkdict = new Dictionary<string, object>();
                foreach (var fke in foreignKeyInfoGetter(tableName))
                {
                    var foreignKey = fke["from"] as string;
                    var primaryKey = fke["to"];
                    var primaryTableName = fke["table"];

                    fkdict[foreignKey] = new Dictionary<string, object>()
                    {
                        { "PrimaryKey", primaryKey },
                        { "PrimaryTableName", primaryTableName }
                    };
                }

                foreach (var col in tableInfoGetter(tableName))
                {
                    // var tableName = result["name"].ToString();
                    var prop = t.GetPropertyFromColumnName(col);
                    var fke = fkdict.ContainsKey(col) ? fkdict[col] as IDictionary<string, object> : null;
                    colinfos.Add(new ColumnInfoEntry() { ColumnName = col, MemberInfo = prop, BindingTo = fke != null ? fke["PrimaryTableName"] as string : null, BindingToPrimaryKey = fke != null ? fke["PrimaryKey"] as string: null });
                    classprops.Remove(prop);
                }

                TypeInfoDict[t] = colinfos.ToArray();

                foreach (var prop in t.GetProperties())
                {
                    if (prop.PropertyType.IsGenericType)
                    {
                        prop.PropertyType.GetGenericArguments()[0].MakeTypePersistent(tableInfoGetter, foreignKeyInfoGetter);
                    }
                    else
                    {
                        prop.PropertyType.MakeTypePersistent(tableInfoGetter, foreignKeyInfoGetter);
                    }
                }

            }
        }

        private static string GetShortColumnName(string fcol)
        {
            return fcol.Split('.').Last();
        }

        public static PropertyInfo GetPropertyFromColumnName(this Type t, string fcol, bool usingPk = false)
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

                if (cc == c || (usingPk && $"{cc}_{p.PropertyType.GetPrimaryKey().ToLower()}" == c))
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

        public static string GetTableName(this Type t)
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
            public string BindingTo;
            public string BindingToPrimaryKey;
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
