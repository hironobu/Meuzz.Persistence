using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
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
            _dict.Add(t, entry);
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
            public Type TargetType;
            public PropertyInfo PropertyInfo;
            public PropertyInfo InversePropertyInfo;
            public string ForeignKey;
            public string PrimaryKey;
        }

        public class Entry
        {
            public Type ClassType;

            public RelationInfoEntry[] Relations;
        }
    }
}
