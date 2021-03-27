using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Meuzz.Persistence.Core
{
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
}
