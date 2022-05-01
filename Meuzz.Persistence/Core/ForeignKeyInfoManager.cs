#nullable enable

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Core
{
    public class ForeignKeyInfoManager
    {
        private IDictionary<Type, string[]>? _typeToForeignKeysTable;
        private IDictionary<PropertyInfo, string>? _propertyInfoToForeignKeyTable;

        public ForeignKeyInfoManager()
        {
        }

        public void InitializeForeignKeyTable()
        {
            var typeToForeignKeysTable = new Dictionary<Type, string[]>();
            var propertyInfoToForeignKeyTable = new Dictionary<PropertyInfo, string>();

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes().Where(t => t.IsDefined(typeof(PersistentClassAttribute), true)))
                {
                    var hasManyProps = type.GetProperties().Where(p => p.IsDefined(typeof(HasManyAttribute), true));
                    foreach (var prop in hasManyProps)
                    {
                        var t = typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType) ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;
                        var hasmany = prop.GetCustomAttribute<HasManyAttribute>();
                        var fk = hasmany?.ForeignKey;
                        if (fk == null)
                        {
                            if (hasManyProps.Where(p => p.PropertyType == prop.PropertyType).Count() > 1)
                            {
                                throw new NotImplementedException();
                            }

                            var revprop = t.GetProperties().Where(x => x.PropertyType == prop.DeclaringType).Single();
                            fk = StringUtils.ToSnake(revprop.Name) + "_id";
                        }
                        propertyInfoToForeignKeyTable.Add(prop, fk);

                        if (typeToForeignKeysTable.ContainsKey(t))
                        {
                            typeToForeignKeysTable[t] = typeToForeignKeysTable[t].Concat(new string[] { fk }).ToArray();
                        }
                        else
                        {
                            typeToForeignKeysTable.Add(t, new string[] { fk });
                        }
                    }
                }
            }

            _typeToForeignKeysTable = typeToForeignKeysTable;
            _propertyInfoToForeignKeyTable = propertyInfoToForeignKeyTable;
        }

        public string[] GetForeignKeysByTargetType(Type targetType)
        {
            return _typeToForeignKeysTable?.ContainsKey(targetType) == true ? _typeToForeignKeysTable[targetType] : new string[] { };
        }

        public string? GetForeignKeyByPropertyInfo(PropertyInfo pi)
        {
            return _propertyInfoToForeignKeyTable?.ContainsKey(pi) == true ? _propertyInfoToForeignKeyTable[pi] : null;
        }

        private Entry GetInversedForeignKeyInfo(Type t, PropertyInfo pi)
        {
            // dummy
            var fki = new Entry(string.Empty, string.Empty, null, null);
            return fki;
        }

        public Entry? GetForeignKeyInfoByPropertyInfo(PropertyInfo pi)
        {
            var pt = pi.PropertyType;
            if (!(typeof(IEnumerable).IsAssignableFrom(pt) && !typeof(string).IsAssignableFrom(pt)))
            {
                if (!pt.IsPersistent())
                {
                    return null;
                }
                else
                {
                    return GetInversedForeignKeyInfo(pt, pi);
                }
            }

            var hasmany = pi.GetCustomAttribute<HasManyAttribute>();
            var declaringType = pi.DeclaringType;
            if (declaringType == null) { throw new NotImplementedException(); }
            var primaryKey = hasmany?.PrimaryKey ?? declaringType.GetPrimaryKey();
            if (primaryKey == null) { throw new NotImplementedException(); }
            var fki = new Entry(
                primaryKey,
                declaringType.GetTableName(),
                hasmany?.ForeignKey ?? GetForeignKeyByPropertyInfo(pi),
                pi.PropertyType.GetTableName()
                );

            return fki;
        }

        public class Entry
        {
            public string PrimaryKey { get; }
            public string PrimaryTableName { get; }
            public string? ForeignKey { get; }
            public string? ForeignTableName { get; }

            public Entry(string primaryKey, string primaryTableName, string? foreignKey, string? foreignTableName)
            {
                PrimaryKey = primaryKey;
                PrimaryTableName = primaryTableName;
                ForeignKey = foreignKey;
                ForeignTableName = foreignTableName;
            }
        }

        private static ForeignKeyInfoManager? _instance = null;
        private static readonly object _instanceLocker = new object();

        public static ForeignKeyInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        var instance = new ForeignKeyInfoManager();
                        instance.InitializeForeignKeyTable();

                        _instance = instance;
                    }
                }
            }

            return _instance;
        }
    }
}
