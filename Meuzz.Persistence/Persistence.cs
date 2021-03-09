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
        private Type _defaultParamType = null;

        public void ResetParameters()
        {
            _parameters.Clear();
        }

        public string RegisterParameter(string name, Type t, bool asDefault)
        {
            var k = name;

            if (name != null)
            {
                int i = 1;
                while (_parameters.ContainsKey(k))
                {
                    k = $"{name}{i++}";
                }
                _parameters.Add(k, t);
            }

            if (asDefault)
            {
                _defaultParamType = t ?? _defaultParamType;
                _defaultParamName = name ?? _defaultParamName;
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

        public Type GetDefaultParamType()
        {
            return _defaultParamType;
        }

        public string GetDefaultParamName()
        {
            return _defaultParamName;
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
        public static TableInfo GetTableInfo(this Type t)
        {
            return TableInfoDict[t];
        }

        public static ClassInfo GetClassInfo(this Type t)
        {
            return ClassInfoDict[t];
        }

        private static IDictionary<Type, TableInfo> TableInfoDict { get; } = new Dictionary<Type, TableInfo>();
        private static IDictionary<Type, ClassInfo> ClassInfoDict { get; } = new Dictionary<Type, ClassInfo>();

        public static bool IsPersistent(this Type t)
        {
            return TableInfoDict.TryGetValue(t, out var _);
        }

        public static void MakeTypePersistent(this Type t, Func<string, string[]> tableInfoGetter, Func<string, IDictionary<string, object>[]> foreignKeyInfoGetter)
        {
            if (!t.IsPersistent())
            {
                var colinfos = new List<ColumnInfoEntry>();
                var classprops = t.GetProperties().ToList();
                var tableName = GetTableName(t);
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
                    var c = col.ToLower();
                    var prop = t.GetPropertyInfoFromColumnName(c);
                    var fke = fkdict.ContainsKey(c) ? fkdict[c] as IDictionary<string, object> : null;
                    colinfos.Add(new ColumnInfoEntry() { Name = c, MemberInfo = prop, BindingTo = fke != null ? fke["PrimaryTableName"] as string : null, BindingToPrimaryKey = fke != null ? fke["PrimaryKey"] as string : null });
                    classprops.Remove(prop);
                }

                TableInfoDict[t] = new TableInfo { Columns = colinfos.ToArray() };

                var relinfos = new List<RelationInfoEntry>();
                foreach (var x in classprops)
                {
                    var hasmany = x.GetCustomAttribute<HasManyAttribute>();
                    relinfos.Add(new RelationInfoEntry()
                    {
                        PropertyInfo = x,
                        TargetClassType = x.PropertyType.IsGenericType ? x.PropertyType.GetGenericArguments()[0] : x.PropertyType,
                        ForeignKey = hasmany != null ? hasmany.ForeignKey : null
                    });
                }
                ClassInfoDict[t] = new ClassInfo { Relations = relinfos.ToArray(), ClassType = t };

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

        public static bool HasColumnName(this Type t, string colname)
        {
            foreach (var e in t.GetTableInfo().Columns)
            {
                if (e.Name == colname) {
                    return true; }
                
            }
            return false;
        }

        private static string GetShortColumnName(string fcol)
        {
            return fcol.Split('.').Last();
        }

        public static PropertyInfo GetPropertyInfoFromColumnName(this Type t, string fcol, bool usingPk = false)
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

        public static object GetPrimaryValue(this Type t,  object obj)
        {
            var pkey = t.GetPrimaryKey();
            return GetPropertyValue(t, pkey, obj);
        }

        public static PropertyInfo GetPrimaryPropertyInfo(this Type t)
        {
            var pkey = t.GetPrimaryKey();
            return t.GetPropertyInfo(pkey);
        }
        public static PropertyInfo GetPropertyInfo(this Type t, string propname)
        {
            return t.GetProperty(StringUtils.ToCamel(propname, true));
        }

        public static object GetPropertyValue(this Type t, string propname, object obj)
        {
            var prop = t.GetProperty(StringUtils.ToCamel(propname, true));
            var pval = prop.GetValue(obj);
            if (prop == null) { return null; }

            if (pval is int)
            {
                return default(int) != (int)pval ? pval : null;
            }
            if (pval is long)
            {
                return default(long) != (long)pval ? pval : null;
            }

            return pval;
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

        public static object GetValueForColumnName(this Type t, string c, object obj)
        {
            var propInfo = t.GetPropertyInfoFromColumnName(c);
            return propInfo?.GetValue(obj);
        }

        public static IDictionary<string, object> GetValueDictFromColumnNames(this object obj, string[] cols)
        {
            Type t = obj.GetType();
            return cols.Zip(obj.GetValuesFromColumnNames(cols), (x, y) => new { x, y }).ToDictionary(x => x.x, x => x.y);
        }
        public static IEnumerable<object> GetValuesFromColumnNames(this object obj, string[] cols)
        {
            Type t = obj.GetType();
            return cols.Select(c => t.GetValueForColumnName(c, obj));
        }

        public static string GetForeignKey(this Type t, string prediction, Type primaryType, string primaryKey)
        {
            var ti = t.GetTableInfo();
            return ti.Columns.Where(x => x.Name.StartsWith(prediction)
            && (x.BindingToPrimaryKey == null || x.BindingToPrimaryKey == primaryKey)
            && (x.BindingTo == null || x.BindingTo == primaryType.GetTableName())).Single().Name;
        }

        public class ColumnInfoEntry
        {
            public string Name;
            public MemberInfo MemberInfo;
            public string BindingTo;
            public string BindingToPrimaryKey;
        }

        public class TableInfo
        {
            public ColumnInfoEntry[] Columns;
        }

        public class RelationInfoEntry
        {
            public Type TargetClassType;
            public PropertyInfo PropertyInfo;
            public string ForeignKey;
            public string PrimaryKey;
        }

        public class ClassInfo
        {
            public Type ClassType;

            public RelationInfoEntry[] Relations;
        }
    }


    public static class MemberInfoExtensions
    {
        public static string GetColumnName(this MemberInfo mi)
        {
            var attr = mi.GetCustomAttribute<PersistentPropertyAttribute>();
            if (attr == null || attr.Column == null)
            {
                return StringUtils.ToSnake(mi.Name);
            }

            return attr.Column.ToLower();
        }
    }

    public static class PropertyInfoExtensions
    {
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
