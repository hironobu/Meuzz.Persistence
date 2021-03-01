﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class ObjectRepositoryBase
    {
        protected Connection _connection = null;

        protected object PopulateObject(Type t, IEnumerable<string> cols, IEnumerable<object> vals)
        {
            var props = cols.Select(c => t.GetPropertyFromColumnName(c)).Where(x => x != null).ToArray<PropertyInfo>();

            var bindings = props.Zip(vals, (k, v) => Expression.Bind(k, Expression.Constant(
                Convert.ChangeType(v, k.PropertyType))));

            NewExpression instance = Expression.New(t);
            Expression expr = Expression.MemberInit(instance, bindings);

            var ft = typeof(Func<>).MakeGenericType(t);
            LambdaExpression lambda = Expression.Lambda(ft, expr);
            dynamic func = Convert.ChangeType(lambda.Compile(), ft);
            return func();
        }

        protected void LoadTableInfoForType(Type t)
        {
            if (t.GetCustomAttribute<PersistentClassAttribute>() == null)
                return;

            t.MakeTypePersistent((t) =>
            {
                return _connection.Execute($"PRAGMA table_info('{t}')").Results.Select(x => x["name"].ToString()).ToArray();
            });
        }
        /*
        private IEnumerable<Type> GetTypesWithHelpAttribute(Assembly assembly, Type targetAttribute)
        {
            foreach (Type type in assembly.GetTypes())
            {
                if (type.GetCustomAttributes(targetAttribute, true).Length > 0)
                {
                    yield return type;
                }
            }
        }*/
    }

    public class ObjectRepository<T, I> : ObjectRepositoryBase where T : class, new()
    {
        private SqlBuilder<T> _sqlBuilder;
        private SqlFormatter _formatter;

        public ObjectRepository(Connection conn, SqlBuilder<T> builder, SqlFormatter formatter)
        {
            _connection = conn;
            _sqlBuilder = builder;
            _formatter = formatter;

            _connection.Open();
            LoadTableInfoForType(typeof(T));
        }

        public T Find(I id)
        {
            throw new NotImplementedException();
        }

        public SelectStatement<T> Where(Expression<Func<T, bool>> f)
        {
            return _sqlBuilder.BuildSelect(f, (stmt) =>
            {
                var sql = _formatter.Format(stmt);
                var rset = _connection.Execute(sql);
                return PopulateObjects(typeof(T), rset, stmt.ParamInfo, stmt.ColumnAliasingInfo);
            });
        }

        public T Create()
        {
            throw new NotImplementedException();
        }

        public T New()
        {
            return new T();
        }

        private IEnumerable<T> PopulateObjects(Type t, Connection.ResultSet rset, ParamInfo paramInfo, ColumnAliasingInfo columnAliasingInfo)
        {
            var rows = rset.Results.Select(x =>
            {
                var kvs = x.Select(c => (columnAliasingInfo.GetOriginalColumnName(c.Key).Split('.'), c.Value));
                var d = new Dictionary<string, object>();
                foreach (var (kk, v) in kvs)
                {
                    var dx = d;
                    // foreach (var k in kk.Take(kk.Length - 1))
                    var k = string.Join('.', kk.Take(kk.Length - 1));
                    {
                        var dx0 = dx;
                        if (dx0.TryGetValue(k, out var value))
                        {
                            dx = value as Dictionary<string, object>;
                        }
                        else
                        {
                            dx = new Dictionary<string, object>();
                            dx0[k] = dx;
                        }
                    }
                    dx[kk.Last().ToLower()] = v;
                }

                return d;
            });
            // return rows.Select(x => (T)PopulateObject(t, (x["x"] as IDictionary<string, object>).Keys, (x["x"] as IDictionary<string, object>).Values, typeInfo));

            var resultDict = new Dictionary<string, Dictionary<object, object>>();
            foreach (var row in rows)
            {
                foreach (var (k, v) in row)
                {
                    var d = v as Dictionary<string, object>;
                    var tt = paramInfo.GetParameterTypeByKey(k);
                    var pk = tt.GetPrimaryKey();

                    Dictionary<object, object> dd = null;
                    if (!resultDict.TryGetValue(k, out var val))
                    {
                        dd = new Dictionary<object, object>();
                        resultDict[k] = dd;
                    }
                    else
                    {
                        dd = val as Dictionary<object, object>;
                    }

                    var pkval = d[pk];
                    if (pkval != null && !dd.TryGetValue(pkval, out var _))
                    {
                        dd[pkval] = d;
                    }
                }
            }

            if (resultDict.Count() == 0)
            {
                return new List<T>();
            }

            var defaultKey = paramInfo.GetDefaultParameterKey();
            var defaultType = paramInfo.GetParameterTypeByKey(defaultKey);

            var joinedKey = "t";

            var primaryResults = resultDict[defaultKey].Select(x => (T)PopulateObject(defaultType, (x.Value as IDictionary<string, object>).Keys, (x.Value as IDictionary<string, object>).Values));
            var results = primaryResults;
            if (resultDict.ContainsKey(joinedKey)) {
                results = LoadJoinedObjects(primaryResults, resultDict[joinedKey], defaultType, joinedKey, paramInfo);
            }
            return results;
        }

        private IEnumerable<T> LoadJoinedObjects(IEnumerable<T> primaryResults, IDictionary<object, object> joinedResults, Type defaultType, string joinedKey, ParamInfo paramInfo)
        {
            var (foreignKey, primaryKey) = paramInfo.GetBindingByKey(joinedKey);

            var joinedType = paramInfo.GetParameterTypeByKey(joinedKey);
            var joinedMemberInfo = paramInfo.GetMemberInfoByKey(joinedKey);
            Func<dynamic, Func<dynamic, bool>> joiningCondition = (dynamic lval) => (dynamic r) => r[foreignKey] == lval;

            var primaryKeyProperty = defaultType.GetProperty(StringUtils.Snake2Camel(primaryKey, true));

            IEnumerable<T> results = null;

            if (joinedMemberInfo != null && joinedMemberInfo is PropertyInfo propInfo)
            {
                results = primaryResults.Select(x =>
                {
                    var cond = joiningCondition(primaryKeyProperty.GetValue(x));
                    var ts = joinedResults.Values
                        .Where(cond)
                        .Select(y => PopulateObject(joinedType, (y as IDictionary<string, object>).Keys, (y as IDictionary<string, object>).Values)).ToArray();
                    var ts2 =
                        typeof(Enumerable)
                            .GetMethod("Cast")
                            .MakeGenericMethod(joinedType)
                            .Invoke(null, new object[] { ts });
                    propInfo.SetValue(x, ts2);
                    return x;
                });
            }

            return results;
        }
    }

    //     public class ObjectRepository<T>  : ObjectRepository<T, int>  where T new() { }

    public static class PersistentObjectExtensions
    {
        public static bool Save(this object obj)
        {
            throw new NotImplementedException();
        }
    }

}
