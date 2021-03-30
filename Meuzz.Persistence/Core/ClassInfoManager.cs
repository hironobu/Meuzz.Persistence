using Meuzz.Foundation;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
    public class ClassInfoManager
    {
        private ConcurrentDictionary<Type, Entry> _dict = null;

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

            var ti = new ClassInfoManager.Entry() { Columns = colinfos.ToArray(), Relations = relinfos.ToArray(), ClassType = t };
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

        private static ClassInfoManager _instance = null;
        private static readonly object _instanceLocker = new Object();

        public class RelationInfoEntry
        {
            public Type TargetType;
            public PropertyInfo PropertyInfo;
            public PropertyInfo InversePropertyInfo;
            public string ForeignKey;
            public string PrimaryKey;
        }

        public class ColumnInfoEntry
        {
            public string Name;
            public MemberInfo MemberInfo;
            public string BindingTo;
            public string BindingToPrimaryKey;
        }


        public class Entry
        {
            public Type ClassType;

            public ColumnInfoEntry[] Columns;
            public RelationInfoEntry[] Relations;
        }
    }
}
