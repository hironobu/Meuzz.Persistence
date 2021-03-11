using System;
using System.Collections.Concurrent;
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


    public class TableInfoManager
    {
        private IDictionary<Type, Entry> _dict = null;

        public TableInfoManager()
        {
            _dict = new ConcurrentDictionary<Type, Entry>();
        }

        public Entry GetEntry(Type t)
        {
            if (!_dict.TryGetValue(t, out var entry))
            {
                return null;
            }
            return entry;
        }

        public bool RegisterEntry(Type t, Entry ti)
        {
            _dict[t] = ti;
            return true;
        }

        public static TableInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        _instance = new TableInfoManager();
                    }
                }
            }

            return _instance;
        }

        private static TableInfoManager _instance = null;
        private static readonly object _instanceLocker = new object();

        public class ColumnInfoEntry
        {
            public string Name;
            public MemberInfo MemberInfo;
            public string BindingTo;
            public string BindingToPrimaryKey;
        }

        public class Entry
        {
            public ColumnInfoEntry[] Columns;
        }
    }

    public class ClassInfoManager
    {
        private IDictionary<Type, Entry> _dict = null;

        public ClassInfoManager()
        {
            _dict = new ConcurrentDictionary<Type, Entry>();
        }

        public Entry GetEntry(Type t)
        {
            if (!_dict.TryGetValue(t, out var entry))
            {
                return null;
            }
            return entry;
        }

        public bool RegisterEntry(Type t, Entry entry)
        {
            _dict[t] = entry;
            return true;
        }

        public static ClassInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        _instance = new ClassInfoManager();
                    }
                }
            }

            return _instance;
        }

        private static ClassInfoManager _instance = null;
        private static readonly object _instanceLocker = new Object();


        public class RelationInfoEntry
        {
            public Type TargetClassType;
            public PropertyInfo PropertyInfo;
            public string ForeignKey;
            public string PrimaryKey;
        }

        public class Entry
        {
            public Type ClassType;

            public RelationInfoEntry[] Relations;
        }

    }


    public class ForeignKeyInfoManager
    {
        private IDictionary<Type, string[]> _foreignKeyTable = null;

        public ForeignKeyInfoManager()
        {
        }

        public void InitializeForeignKeyTable()
        {
            var table = new Dictionary<Type, string[]>();
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes().Where(t => t.IsDefined(typeof(PersistentClassAttribute), true)))
                {
                    var hasManyProps = type.GetProperties().Where(p => p.IsDefined(typeof(HasManyAttribute), true));
                    foreach (var prop in hasManyProps)
                    {
                        var t = typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType) ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;
                        var hasmany = prop.GetCustomAttribute<HasManyAttribute>();
                        var fk = hasmany.ForeignKey;
                        if (fk == null)
                        {
                            if (hasManyProps.Where(p => p.PropertyType == prop.PropertyType).Count() > 1)
                            {
                                throw new NotImplementedException();
                            }

                            var revprop = t.GetProperties().Where(x => x.PropertyType == prop.DeclaringType).Single();
                            fk = StringUtils.ToSnake(revprop.Name) + "_id";
                        }

                        if (table.ContainsKey(t))
                        {
                            table[t] = table[t].Concat(new string[] { fk }).ToArray();
                        }
                        else
                        {
                            table.Add(t, new string[] { fk });
                        }
                    }
                }
            }
            _foreignKeyTable = table;
        }

        public string[] GetForeignKeysByTargetType(Type targetType)
        {
            if (_foreignKeyTable == null)
            {
                InitializeForeignKeyTable();
            }

            if (_foreignKeyTable.ContainsKey(targetType))
            {
                return _foreignKeyTable[targetType];
            }
            else
            {
                return new string[] { };
            }
        }

        private static ForeignKeyInfoManager _instance = null;
        private static readonly object _instanceLocker = new object();

        public static ForeignKeyInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        _instance = new ForeignKeyInfoManager();
                        _instance.InitializeForeignKeyTable();
                    }
                }
            }

            return _instance;
        }
    }

    public static class TypeInfoExtensions
    {
        public static TableInfoManager.Entry GetTableInfo(this Type t)
        {
            if (!t.IsPersistent())
            {
                throw new NotImplementedException();
            }

            var ti = TableInfoManager.Instance().GetEntry(t);
            if (ti != null)
            {
                return ti;
            }

            var colinfos = new List<TableInfoManager.ColumnInfoEntry>();
            foreach (var prop in t.GetProperties())
            {
                var fke = prop.GetForeignKeyInfo();

                if (fke != null)
                {

                }
                else
                {
                    colinfos.Add(new TableInfoManager.ColumnInfoEntry()
                    {
                        Name = StringUtils.ToSnake(prop.Name),
                        MemberInfo = prop,
                        BindingTo = fke != null ? fke.PrimaryTableName : null,
                        BindingToPrimaryKey = fke != null ? fke.PrimaryKey : null
                    });
                }
            }

            var fkeys = ForeignKeyInfoManager.Instance().GetForeignKeysByTargetType(t);
            foreach (var fk in fkeys)
            {
                colinfos.Add(new TableInfoManager.ColumnInfoEntry()
                {
                    Name = StringUtils.ToSnake(fk),
                });
            }

            ti = new TableInfoManager.Entry() { Columns = colinfos.ToArray() };
            TableInfoManager.Instance().RegisterEntry(t, ti);

            return ti;
        }

        // private static IDictionary<string, object> _tableInfo = new ConcurrentDictionary<string, object>();

        [Obsolete("TO BE REVIEWED")]
        public static ClassInfoManager.Entry GetClassInfo(this Type t)
        {
            if (!t.IsPersistent())
            {
                throw new NotImplementedException();
            }

            var ti = ClassInfoManager.Instance().GetEntry(t);
            if (ti != null)
            {
                return ti;
            }

            var relinfos = new List<ClassInfoManager.RelationInfoEntry>();
            foreach (var prop in t.GetProperties())
            {
                var hasmany = prop.GetCustomAttribute<HasManyAttribute>();
                relinfos.Add(new ClassInfoManager.RelationInfoEntry()
                {
                    PropertyInfo = prop,
                    TargetClassType = prop.PropertyType.IsGenericType ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType,
                    ForeignKey = hasmany != null ? hasmany.ForeignKey : null
                });
            }

            ti = new ClassInfoManager.Entry() { Relations = relinfos.ToArray(), ClassType = t };
            ClassInfoManager.Instance().RegisterEntry(t, ti);

            return ti;
        }

        public static bool IsPersistent(this Type t)
        {
            // return TableInfoManager.Instance().TryGetEntry(t, out var _);
            return t.GetCustomAttribute<PersistentClassAttribute>() != null;
        }

        /**
         * 
         * DBからテーブル情報を取得し、TableInfoManagerおよびClassInfoManagerにその内容を登録する。
         * ただし、DBのスキーマ情報をどの程度の優先度をもってTableInfoManagerおよびClassInfoManager上で扱うかについてを精査しないといけないので、
         * 両クラスでの情報生成・登録のロジックを見直す必要がある。
         * 
         * 1) モデルクラスの命名規則
         * 2) モデルクラスにおけるアノテーション
         * 3) DBのテーブル情報
         * 
         */
        public static void MakeTypePersistent(this Type t, Func<string, string[]> tableInfoGetter, Func<string, IDictionary<string, object>[]> foreignKeyInfoGetter)
        {/*
            if (!t.IsPersistent())
            {
                var colinfos = new List<TableInfoManager.ColumnInfoEntry>();
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
                    colinfos.Add(new TableInfoManager.ColumnInfoEntry() { Name = c, MemberInfo = prop, BindingTo = fke != null ? fke["PrimaryTableName"] as string : null, BindingToPrimaryKey = fke != null ? fke["PrimaryKey"] as string : null });
                    classprops.Remove(prop);
                }

                TableInfoManager.Instance().RegisterEntry(t, new TableInfoManager.Entry() { Columns = colinfos.ToArray() });

                var relinfos = new List<ClassInfoManager.RelationInfoEntry>();
                foreach (var x in classprops)
                {
                    var hasmany = x.GetCustomAttribute<HasManyAttribute>();
                    relinfos.Add(new ClassInfoManager.RelationInfoEntry()
                    {
                        PropertyInfo = x,
                        TargetClassType = x.PropertyType.IsGenericType ? x.PropertyType.GetGenericArguments()[0] : x.PropertyType,
                        ForeignKey = hasmany != null ? hasmany.ForeignKey : null
                    });
                }

                ClassInfoManager.Instance().RegisterEntry(t, new ClassInfoManager.Entry() { ClassType = t, Relations = relinfos.ToArray() });

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

            }*/
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





        public class ForeignKeyInfo
        {
            public string PrimaryKey;
            public string PrimaryTableName;
            public string ForeignKey;
            public string ForeignTableName;
        }

        private static ForeignKeyInfo GetForeignKeyInfoReversed(Type t, PropertyInfo pi)
        {
            // dummy
            var fki = new ForeignKeyInfo();
            return fki;
        }

        public static ForeignKeyInfo GetForeignKeyInfo(this PropertyInfo pi)
        {
            var pt = pi.PropertyType;
            if (!(typeof(System.Collections.IEnumerable).IsAssignableFrom(pt) && !typeof(string).IsAssignableFrom(pt)))
            {
                if (!pt.IsPersistent())
                {
                    return null;
                }
                else
                {
                    return GetForeignKeyInfoReversed(pt, pi);
                }
            }

            var fki = new ForeignKeyInfo();

            var hasmany = pi.GetCustomAttribute<HasManyAttribute>();
            fki.PrimaryKey = hasmany?.PrimaryKey ?? pi.DeclaringType.GetPrimaryKey();
            fki.PrimaryTableName = pi.DeclaringType.GetTableName();
            fki.ForeignKey = hasmany?.ForeignKey;
            fki.ForeignTableName = pi.PropertyType.GetTableName();

            return fki;
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

        public PersistentClassAttribute(string tableName) : this(tableName, "id")
        {
        }

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
        public string ForeignKey = null;
        public string PrimaryKey = null;

        public HasManyAttribute(string ForeignKey = null, string PrimaryKey = null)
        {
            this.ForeignKey = ForeignKey;
            this.PrimaryKey = PrimaryKey;
        }
    }
}
