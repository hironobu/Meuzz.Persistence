#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Core
{
    public class ClassInfoManager
    {
        private ConcurrentDictionary<Type, Entry> _dict;

        public ClassInfoManager()
        {
            _dict = new ConcurrentDictionary<Type, Entry>();
        }

        public Entry GetEntry(Type t)
        {
            if (!_dict.TryGetValue(t, out var entry))
            {
                var ti = MakeReadyEntryForType(t);
                return ti;
            }
            return entry;
        }

        private Entry MakeReadyEntryForType(Type t)
        {
            var colinfos = new List<ClassInfoManager.ColumnInfoEntry>();
            var relinfos = new List<ClassInfoManager.RelationInfoEntry>();
            foreach (var prop in t.GetProperties())
            {
                var fke = ForeignKeyInfoManager.Instance().GetForeignKeyInfoByPropertyInfo(prop);

                if (fke != null)
                {
                    if (fke.ForeignKey != null)
                    {
                        var targetType = prop.PropertyType.IsGenericType ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;
                        relinfos.Add(new ClassInfoManager.RelationInfoEntry(targetType, prop, targetType.GetPropertyInfoFromColumnName(fke.ForeignKey, true), fke.ForeignKey));
                   }
                }
                else
                {
                    colinfos.Add(new ClassInfoManager.ColumnInfoEntry(
                        StringUtils.ToSnake(prop.Name),
                        prop,
                        fke)
                    );
                }
            }

            var fkeys = ForeignKeyInfoManager.Instance().GetForeignKeysByTargetType(t);
            foreach (var fk in fkeys)
            {
                colinfos.Add(new ClassInfoManager.ColumnInfoEntry(StringUtils.ToSnake(fk)));
            }

            var ti = new ClassInfoManager.Entry(t, colinfos.ToArray(), relinfos.ToArray());
            _dict.TryAdd(t, ti);

            return ti;
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

        private static ClassInfoManager? _instance = null;
        private static readonly object _instanceLocker = new Object();

        public class RelationInfoEntry
        {
            public Type TargetType { get; }
            public PropertyInfo PropertyInfo { get;  }
            public PropertyInfo InversePropertyInfo { get; }
            public string ForeignKey { get; }
            public string? PrimaryKey { get; }

            public RelationInfoEntry(Type targetType, PropertyInfo propertyInfo, PropertyInfo inversePropertyInfo, string foreignKey, string? primaryKey = null)
            {
                TargetType = targetType;
                PropertyInfo = propertyInfo;
                InversePropertyInfo = inversePropertyInfo;
                ForeignKey = foreignKey;
                PrimaryKey = primaryKey;
            }
        }

        public class ColumnInfoEntry
        {
            public string Name { get; }
            public MemberInfo? MemberInfo { get; }
            public string? BindingTo { get; }
            public string? BindingToPrimaryKey { get; }

            public ColumnInfoEntry(string name, MemberInfo? memberInfo = null, ForeignKeyInfoManager.Entry? fke = null)
            {
                Name = name;
                MemberInfo = memberInfo;
                BindingTo = fke?.PrimaryTableName;
                BindingToPrimaryKey = fke?.PrimaryKey;
            }
        }


        public class Entry
        {
            public Type ClassType { get; }

            public ColumnInfoEntry[] Columns { get; }
            public RelationInfoEntry[] Relations { get; }

            public Entry(Type classType, ColumnInfoEntry[] columns, RelationInfoEntry[] relations)
            {
                ClassType = classType;
                Columns = columns;
                Relations = relations;
            }
        }
    }
}
