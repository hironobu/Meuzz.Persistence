#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;

namespace Meuzz.Persistence
{
    public static class TypeInfoExtensions
    {/*
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
        }*/

        // private static IDictionary<string, object> _tableInfo = new ConcurrentDictionary<string, object>();

        public static ClassInfoManager.Entry? GetClassInfo(this Type t)
        {
            if (!t.IsPersistent())
            {
                // throw new NotImplementedException();
            }

            var ti = ClassInfoManager.Instance().GetEntry(t);
            if (ti != null)
            {
                return ti;
            }

            /*var colinfos = new List<ClassInfoManager.ColumnInfoEntry>();
            var relinfos = new List<ClassInfoManager.RelationInfoEntry>();
            foreach (var prop in t.GetProperties())
            {
                var fke = prop.GetForeignKeyInfo();

                if (fke != null)
                {
                    if (fke.ForeignKey != null)
                    {
                        var targetType = prop.PropertyType.IsGenericType ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;
                        relinfos.Add(new ClassInfoManager.RelationInfoEntry()
                        {
                            PropertyInfo = prop,
                            InversePropertyInfo = targetType.GetPropertyInfoFromColumnName(fke.ForeignKey, true),
                            TargetType = targetType,
                            ForeignKey = fke.ForeignKey
                        });
                    }
                }
                else
                {
                    colinfos.Add(new ClassInfoManager.ColumnInfoEntry()
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
                colinfos.Add(new ClassInfoManager.ColumnInfoEntry()
                {
                    Name = StringUtils.ToSnake(fk),
                });
            }

            ti = new ClassInfoManager.Entry() { Columns = colinfos.ToArray(), Relations = relinfos.ToArray(), ClassType = t };
            ClassInfoManager.Instance().RegisterEntry(t, ti);*/

            return null;
        }

        public static bool IsPersistent(this Type t)
        {
            // return TableInfoManager.Instance().TryGetEntry(t, out var _);
            return t.GetCustomAttribute<PersistentAttribute>() != null;
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

        public static PropertyInfo? GetPropertyInfoFromColumnName(this Type t, string fcol, bool usingPk = false)
        {
            var c = GetShortColumnName(fcol).ToLower();
            foreach (var p in t.GetProperties())
            {
                var cc = p.Name.ToSnake().ToLower();
                var ppa = p.GetCustomAttribute<ColumnAttribute>();
                if (ppa != null && ppa.Name != null)
                {
                    cc = ppa.Name.ToLower();
                }

                var primaryKey = p.PropertyType.GetPrimaryKey() ?? "id";
                if (cc == c || (usingPk && $"{cc}_{primaryKey.ToLower()}" == c))
                {
                    return p;
                }
            }

            return null;
        }

        public static string? GetPrimaryKey(this Type t)
        {
            var attr = t.GetCustomAttribute<PersistentAttribute>();
            if (attr != null && attr.PrimaryKey != null)
            {
                return attr.PrimaryKey;
            }
            return null;
        }

        public static object? GetPrimaryValue(this Type t,  object obj)
        {
            var pkey = t.GetPrimaryKey();
            if (pkey == null) { return null; }
            return GetPropertyValue(t, pkey, obj);
        }

        public static PropertyInfo? GetPrimaryPropertyInfo(this Type t)
        {
            var pkey = t.GetPrimaryKey();
            if (pkey == null) { return null; }
            return t.GetPropertyInfo(pkey);
        }
        public static PropertyInfo? GetPropertyInfo(this Type t, string propname)
        {
            return t.GetProperty(propname.ToCamel(true));
        }

        public static object? GetPropertyValue(this Type t, string propname, object obj)
        {
            var prop = t.GetProperty(propname.ToCamel(true));
            if (prop == null) { return null; }

            var pval = prop.GetValue(obj);
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
            var attr = t.GetCustomAttribute<PersistentAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return t.Name.ToSnake();
            }
            return attr.TableName;
        }

        public static object? GetValueForColumnName(this Type t, string c, object obj)
        {
            var propInfo = t.GetPropertyInfoFromColumnName(c);
            return propInfo?.GetValue(obj);
        }

        public static IDictionary<string, object?> GetValueDictFromColumnNames(this Type t, string[] cols, object obj)
        {
            return cols.Zip(t.GetValuesFromColumnNames(cols, obj), (x, y) => new { x, y }).ToDictionary(x => x.x, x => x.y);
        }
        public static IEnumerable<object?> GetValuesFromColumnNames(this Type t, string[] cols, object obj)
        {
            return cols.Select(c => t.GetValueForColumnName(c, obj));
        }

        public static string GetForeignKey(this Type t, string prediction, Type primaryType, string primaryKey)
        {
            var ci = t.GetClassInfo();
            if (ci == null) { throw new NotImplementedException(); }
            return ci.Columns.Where(x => x.Name.StartsWith(prediction)
                && (x.BindingToPrimaryKey == null || x.BindingToPrimaryKey == primaryKey)
                && (x.BindingTo == null || x.BindingTo == primaryType.GetTableName())).Single().Name;
        }
    }


    public static class MemberInfoExtensions
    {
        public static string GetColumnName(this MemberInfo mi)
        {
            var attr = mi.GetCustomAttribute<ColumnAttribute>();
            if (attr == null || attr.Name == null)
            {
                return mi.Name.ToSnake();
            }

            return attr.Name.ToLower();
        }
    }
}
