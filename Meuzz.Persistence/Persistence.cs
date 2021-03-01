﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class ParamInfo
    {
        private IDictionary<string, Type> _parameters = new Dictionary<string, Type>();
        private string _defaultParameterKey = null;

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
                _defaultParameterKey = k;
            }
            else
            {
                _memberInfos.Add(k, memberInfo);
            }

            return k;
        }

        public Type GetParameterTypeByKey(string key)
        {
            return _parameters[key];
        }


        public MemberInfo GetMemberInfoByKey(string key)
        {
            return _memberInfos[key];
        }


        public string GetDefaultParameterKey()
        {
            return _defaultParameterKey;
        }


        public (string, string) GetBindingByKey(string key)
        {
            return (_bindings[key][0], _bindings[key][1]);
        }

        public void SetBindingByKey(string key, string foreignKey, string primaryKey)
        {
            _bindings.Add(key, new string[] { foreignKey, primaryKey });
        }

        private IDictionary<string, string[]> _bindings = new Dictionary<string, string[]>();


    }


    public class ColumnAliasingInfo
    {
        private IDictionary<string, string> _aliasingProperties = new Dictionary<string, string>();

        public IDictionary<string, string> MakeColumnAliasingDictionary(string paramKey, IEnumerable<string> props)
        {
            var propKeys = props.Select(x => $"{paramKey}.{x}");
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


    public class TypeInfo
    {
        public IEnumerable<string> GetColumnsFromType(Type t)
        {
            return TypeInfoDict[t].Select(x => x.ColumnName);
        }

        public IDictionary<Type, ColumnInfoEntry[]> TypeInfoDict { get; set; } = new Dictionary<Type, ColumnInfoEntry[]>();

        private string GetShortColumnName(string fcol)
        {
            return fcol.Split('.').Last();
        }

        public PropertyInfo GetPropertyFromColumnName(Type t, string fcol)
        {
            var c = GetShortColumnName(fcol).ToLower();
            foreach (var p in t.GetProperties())
            {
                var cc = StringUtils.Camel2Snake(p.Name).ToLower();
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

        public string GetPrimaryKey(Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr != null && attr.PrimaryKey != null)
            {
                return attr.PrimaryKey;
            }
            return "id";
        }

        public TypeInfo ForStatement()
        {
            return new TypeInfo() { TypeInfoDict = this.TypeInfoDict };
        }


        [Obsolete("MIGHT BE HERE")]
        public string GetTableNameFromClassName(Type t)
        {
            var attr = t.GetCustomAttribute<PersistentClassAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return StringUtils.Camel2Snake(t.Name);
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