using System;
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
                //TODO: for sqlite only
                return _connection.Execute($"PRAGMA table_info('{t}')").Results.Select(x => x["name"].ToString()).ToArray();
            }, (t) =>
            {
                return _connection.Execute($"PRAGMA foreign_key_list('{t}')").Results.ToArray();
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

    public class ObjectRepository<T> : ObjectRepository<T, object> where T: class, new()
    {
        public ObjectRepository(Connection conn, SqlBuilder<T> builder, SqlFormatter formatter, SqlCollator collator) : base(conn, builder, formatter, collator) { }
    }

    public class ObjectRepository<T, I> : ObjectRepositoryBase where T : class, new()
    {
        private SqlBuilder<T> _sqlBuilder;
        private SqlFormatter _formatter;
        private SqlCollator _collator;

        public ObjectRepository(Connection conn, SqlBuilder<T> builder, SqlFormatter formatter, SqlCollator collator)
        {
            _connection = conn;
            _sqlBuilder = builder;
            _formatter = formatter;
            _collator = collator;

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
                var sql = _formatter.Format(stmt, out var context);
                var rset = _connection.Execute(sql, context);
                return PopulateObjects(rset, stmt, context);
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

        public class MyEqualityComparer : IEqualityComparer<object>
        {
            private bool IsNumeric(object x)
            {
                return x is int || x is long || x is uint || x is ulong;
            }

            public new bool Equals(object x, object y)
            {
                if (IsNumeric(x) && IsNumeric(y))
                {
                    return x.Equals(Convert.ToInt64(y));
                }
                return x.Equals(y);
            }

            public int GetHashCode(object obj)
            {
                return obj.GetHashCode();
            }
        }

        private IEnumerable<T> PopulateObjects(Connection.ResultSet rset, SqlSelectStatement statement, SqlConnectionContext context)
        {
            var rows = rset.Results.Select(x =>
            {
                var xx = _collator.Collate(x, context);
                var kvs = xx.Select(c => (c.Key.Split('.'), c.Value));
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

            var resultDict = new Dictionary<string, IDictionary<dynamic, IDictionary<string, object>>>();
            var resultObjects = new Dictionary<string, IDictionary<dynamic, object>>();

            foreach (var row in rows)
            {
                foreach (var (k, v) in row)
                {
                    var d = v as Dictionary<string, object>;
                    var tt = statement.ParamInfo.GetParameterTypeByParamName(k);
                    var pk = tt.GetPrimaryKey();

                    IDictionary<object, IDictionary<string, object>> dd = null;
                    if (!resultDict.TryGetValue(k, out var val))
                    {
                        dd = new Dictionary<object, IDictionary<string, object>>();
                        resultDict[k] = dd;
                    }
                    else
                    {
                        dd = val as IDictionary<object, IDictionary<string, object>>;
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

            var objectTree = new Dictionary<string, Dictionary<string, Dictionary<dynamic, List<object>>>>();
            foreach (var x in resultDict.Keys)
            {
                foreach (var binding in statement.GetBindingsForPrimaryParamName(x))
                {
                    if (!objectTree.TryGetValue(binding.ForeignParamName, out var d))
                    {
                        d = new Dictionary<string, Dictionary<dynamic, List<object>>>();
                        objectTree.Add(binding.ForeignParamName, d);
                    }

                    if (!d.TryGetValue(binding.ForeignKey, out var dd))
                    {
                        dd = new Dictionary<dynamic, List<object>>(new MyEqualityComparer());
                        d.Add(binding.ForeignKey, dd);
                    }
                }
            }

            Func<Type, IEnumerable<object>, IEnumerable<object>> regularCollection
                = (t, objs) => (IEnumerable<object>)typeof(Enumerable)
                    .GetMethod("Cast")
                    .MakeGenericMethod((t.IsGenericType) ? t.GetGenericArguments()[0] : t)
                    .Invoke(null, new object[] { objs });

            foreach (var (k, v) in resultDict) {
                var t = statement.ParamInfo.GetParameterTypeByParamName(k);
                var objs = resultDict[k].Select(x =>
                {
                    var v = x.Value;
                    var o = PopulateObject(t, v.Keys, v.Values);
                    if (objectTree.ContainsKey(k))
                    {
                        foreach (var (kk, vv) in v)
                        {
                            if (!objectTree.ContainsKey(k) || !objectTree[k].ContainsKey(kk))
                            {
                                continue;
                            }

                            if (!objectTree[k][kk].TryGetValue(vv, out var os))
                            {
                                os = new List<object>();
                                objectTree[k][kk].Add(vv, os);
                            }
                            os.Add(o);
                        }
                    }
                    return o;
                });
                var primaryKeyValue = t.GetProperty(StringUtils.ToCamel(t.GetPrimaryKey(), true));
                resultObjects.Add(k, objs.ToDictionary(x => primaryKeyValue.GetValue(x), x => x));
            }

            Func<string, Action<dynamic, dynamic>> propertySetter = (string prop) => (dynamic x, dynamic value) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).SetValue(x, value);
            Func<string, Action<dynamic, dynamic>> dictionarySetter = (string key) => (dynamic x, dynamic value) => x[key] = value;
            Action<dynamic, string, dynamic> memberUpdater = (x, memb, value) =>
            {
                Type t = x.GetType();
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    dictionarySetter(memb)(x, value);
                }
                else
                {
                    propertySetter(memb)(x, value);
                }
            };

            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
            Func<string, Func<dynamic, dynamic>> dictionaryGetter = (string key) => (dynamic x) => x[key];
            Func<string, Func<dynamic, dynamic>> memberAccessor = (string memb) => (dynamic x) =>
            {
                Type t = x.GetType();
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    return dictionaryGetter(memb)(x);
                }
                else
                {
                    return propertyGetter(memb)(x);
                }
            };

            foreach (var bindingSpec in statement.GetAllBindings())
            {
                var fromObjs = resultObjects[bindingSpec.PrimaryParamName].Values;
                // var toObjs = resultDict[to].Values;
                Func<object, Func<object, bool>> filteringConditions = (x) => (y) => bindingSpec.Conditions(x, y);

                Func<object, object> fmap = x =>
                {
                    // var targetToObjs = toObjs.Where(filteringConditions(x));
                    // Console.WriteLine(targetToObjs.ToList());
                    var pkv = memberAccessor(bindingSpec.PrimaryKey)(x);
                    if (objectTree[bindingSpec.ForeignParamName][bindingSpec.ForeignKey].TryGetValue(pkv, out List<object> targetToObjs))
                    {
                        foreach (var o in targetToObjs)
                        {
                            // o as IDictionary<string, object>)[$"__{bindingInfo.ForeignKey}"] = x;
                            // memberUpdater(o, bindingInfo.ForeignKey, x);
                            var k0 = StringUtils.ToCamel(bindingSpec.ForeignKey.Replace("_id", ""), true);
                            memberUpdater(o, k0, x);
                        }
                        // (x as IDictionary<string, object>)[$"__{StringUtils.ToSnake(bindingInfo.MemberInfo.Name)}"] = targetToObjs;
                        memberUpdater(x, bindingSpec.MemberInfo.Name, regularCollection((bindingSpec.MemberInfo as PropertyInfo).PropertyType, targetToObjs));
                    }
                    else
                    {
                        memberUpdater(x, bindingSpec.MemberInfo.Name, regularCollection((bindingSpec.MemberInfo as PropertyInfo).PropertyType, MakeGenerator(bindingSpec)));
                    }
                    return x;
                };
                var r = fromObjs.Select(fmap).ToList(); // just do it
                Console.WriteLine(r);
            }

            return (IEnumerable<T>)typeof(Enumerable)
                            .GetMethod("Cast")
                            .MakeGenericMethod(typeof(T))
                            .Invoke(null, new object[] { resultObjects[statement.ParamInfo.GetDefaultParamName()].Values });
/*
            var bindingInfo = paramInfo.GetBindingInfoByName(k);
            var defaultParamName = paramInfo.GetDefaultParamName();
            var defaultType = paramInfo.GetParameterTypeByParamName(defaultParamName);

            var primaryResults = null;

            var results = primaryResults;
            foreach (var (k, v) in resultDict.Where(x => x.Key != defaultParamName))
            {
                if (resultDict.ContainsKey(k))
                {
                    results = LoadJoinedObjects(results, resultDict[k], defaultType, k, paramInfo);
                }
            }
            return results;
*/
        }

        private IEnumerable<object> MakeGenerator(BindingSpec bindingSpec)
        {
            // yield return null;
            yield break;
        }

#if false
        private IEnumerable<T> LoadJoinedObjects(IEnumerable<T> primaryResults, IDictionary<object, IDictionary<string, object>> joinedResults, Type defaultType, string joinedParamName, ParamInfo paramInfo)
        {
            var joinedType = paramInfo.GetParameterTypeByParamName(joinedParamName);
            var joinedMemberInfo = paramInfo.GetMemberInfoByParamName(joinedParamName);
            var bindingInfo = paramInfo.GetBindingInfoByName("x", joinedParamName);
            Func<dynamic, Func<dynamic, bool>> joiningCondition = (x) => (y) => bindingInfo.Conditions(x, y);

            IEnumerable<T> results = null;

            if (joinedMemberInfo != null && joinedMemberInfo is PropertyInfo propInfo)
            {
                results = primaryResults.Select(x =>
                {
                    var ts = joinedResults.Values
                        .Where(joiningCondition(x))
                        .Select(y =>
                        {
                            // var foreignPropertyInfo = joinedType.GetPropertyFromColumnName(foreignKey, true);
                            var obj = PopulateObject(joinedType, y.Keys, y.Values);
                            // foreignPropertyInfo.SetValue(obj, x);
                            return obj;
                        }).ToArray();
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
#endif
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
