using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Persistence.Core;
using Meuzz.Persistence.Reflections;
using Meuzz.Persistence.Sql;
using Meuzz.Foundation;

namespace Meuzz.Persistence
{
    public class ObjectRepositoryBase
    {
        protected Connection _connection = null;

        protected SqlFormatter _formatter;
        protected SqlCollator _collator;

        protected IEnumerable<object> LoadObjects(Type t, SqlSelectStatement statement, Action<IEnumerable<object>> propertySetter = null)
        {
            var (sql, parameters) = _formatter.Format(statement, out var context);
            var rset = _connection.Execute(sql, parameters, context);

            var results = PopulateObjects(t, rset, statement, context);
            if (propertySetter != null)
            {
                propertySetter(results);
            }

            foreach (var o in results)
            {
                yield return o;
            }
            yield break;
        }

        protected IEnumerable<object> MakeDefaultReverseLoader(object value, Type targetType)
        {
            var statement = new SqlSelectStatement(targetType);
            statement.BuildCondition(targetType.GetPrimaryKey(), value);

            return LoadObjects(targetType, statement);
        }

        protected IEnumerable<object> MakeDefaultLoader(object obj, ClassInfoManager.RelationInfoEntry reli)
        {
            var statement = new SqlSelectStatement(reli.TargetType);
            statement.BuildCondition(reli.ForeignKey, obj.GetType().GetPrimaryValue(obj));

            return LoadObjects(reli.TargetType, statement, (results) =>
            {
                var t = reli.TargetType;
                var conv = typeof(Enumerable)
                    .GetMethod("Cast")
                    .MakeGenericMethod((t.IsGenericType) ? t.GetGenericArguments()[0] : t);
                if (reli.InversePropertyInfo != null)
                {
                    foreach (var x in results)
                    {
                        reli.InversePropertyInfo.SetValue(x, obj);
                    }
                }
                reli.PropertyInfo.SetValue(obj, conv.Invoke(null, new object[] { results }));
            });
        }

        protected object PopulateObject(Type t, IEnumerable<string> cols, IEnumerable<object> vals)
        {
            Func<PropertyInfo, object, MemberAssignment> mapper = (k, v) =>
            {
                return Expression.Bind(k, Expression.Constant(Convert.ChangeType(v, k.PropertyType)));
            };
            var bindings = new List<MemberAssignment>();

            // var proxyTypeBuilder = new PersistentTypeBuilder();
            // proxyTypeBuilder.BuildStart(Assembly.GetExecutingAssembly().GetName(), t);
            IDictionary<PropertyInfo, IEnumerable<object>> reverseLoaders = new Dictionary<PropertyInfo, IEnumerable<object>>();

            foreach (var (c, v) in cols.Zip(vals))
            {
                var prop = t.GetPropertyInfoFromColumnName(c, true);
                if (prop == null)
                {
                    continue;
                }

                if (prop.PropertyType.IsPersistent())
                {
                    // bindings.Add(mapper(prop, null));
                    // proxyTypeBuilder.BuildOverrideProperty(prop);
                    if (v != null)
                    {
                        var loader = MakeDefaultReverseLoader(v, prop.PropertyType);
                        reverseLoaders.Add(prop, loader);
                    }
                }
                else
                {
                    bindings.Add(mapper(prop, v));
                }
            };

            var ci = t.GetClassInfo();

            NewExpression instance = Expression.New(t);
            Expression expr = Expression.MemberInit(instance, bindings);

            var ft = typeof(Func<>).MakeGenericType(t);
            LambdaExpression lambda = Expression.Lambda(ft, expr);
            dynamic func = Convert.ChangeType(lambda.Compile(), ft);
            var obj = func();

            foreach (var reli in ci.Relations)
            {
                var prop = reli.PropertyInfo;
                if (prop != null)
                {
                    var tt = prop.PropertyType;
                    var conv = typeof(Enumerable)
                        .GetMethod("Cast")
                        .MakeGenericMethod((tt.IsGenericType) ? tt.GetGenericArguments()[0] : tt);

                    prop.SetValue(obj, conv.Invoke(null, new object[] { MakeDefaultLoader(obj, reli) }));
                }
            }

            foreach (var (prop, proploader) in reverseLoaders)
            {
                var loaderField = obj.GetType().GetField($"__load_{prop.Name}", BindingFlags.NonPublic | BindingFlags.Instance);

                var tt = prop.PropertyType;
                var conv = typeof(Enumerable)
                    .GetMethod("Cast")
                    .MakeGenericMethod((tt.IsGenericType) ? tt.GetGenericArguments()[0] : tt);

                loaderField.SetValue(obj, conv.Invoke(null, new object[] { proploader }));
            }

            PersistenceContext.Generate(obj); // for reset
            return obj;
        }

        protected bool StoreObjects(Type t, IEnumerable<object> objs, IDictionary<string, object> extraData)
        {
            var updated = objs.Where(x => t.GetPrimaryValue(x) != null).ToList();
            var inserted = objs.Where(x => t.GetPrimaryValue(x) == null).ToList();

            if (inserted.Count() > 0)
            {
                var tt = typeof(InsertStatement<>).MakeGenericType(t);
                dynamic insertStatement = Convert.ChangeType(Activator.CreateInstance(tt), tt);
                insertStatement.Append(inserted);
                insertStatement.ExtraData = extraData;
                var (sql, parameters) = _formatter.Format(insertStatement as SqlStatement, out SqlConnectionContext context);
                var rset = _connection.Execute(sql, parameters, context);

                var pkey = t.GetPrimaryKey();
                var prop = t.GetProperty(StringUtils.ToCamel(pkey, true));
                var classinfo = t.GetClassInfo();

                int newPrimaryId = (int)Convert.ChangeType(rset.Results.First()["new_id"], prop.PropertyType) - inserted.Count() + 1;

                foreach (var (y, i) in inserted.Select((x, i) => (x, i)))
                {
                    prop.SetValue(y, newPrimaryId + i);

                    foreach (var rel in classinfo.Relations)
                    {
                        var foreignType = rel.TargetType;
                        var childObjs = rel.PropertyInfo.GetValue(y) as IEnumerable<object>;

                        if (childObjs != null)
                        {
                            StoreObjects(foreignType, childObjs, new Dictionary<string, object>() { { rel.ForeignKey, newPrimaryId } });
                        }
                    }
                }
            }

            if (updated.Count() > 0)
            {
                var tt = typeof(UpdateStatement<>).MakeGenericType(t);
                dynamic updateStatement = Convert.ChangeType(Activator.CreateInstance(tt), tt);
                updateStatement.Append(updated);
                var (sql2, parameters) = _formatter.Format(updateStatement as SqlStatement, out SqlConnectionContext context2);
                _connection.Execute(sql2, parameters, context2);
            }

            return true;
        }

        protected IEnumerable<object> PopulateObjects(Type t, Connection.ResultSet rset, SqlSelectStatement statement, SqlConnectionContext context)
        {
            var rows = rset.Results.Select(x =>
            {
                var xx = _collator.Collate(x, context);
                var kvs = xx.Select(c => (c.Key.Split('.'), c.Value));
                var d = new Dictionary<string, object>();
                foreach (var (kk, v) in kvs)
                {
                    var dx = d;
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
            var resultObjects = new Dictionary<string, IDictionary<dynamic, IDictionary<string, object>>>();

            foreach (var row in rows)
            {
                foreach (var (k, v) in row)
                {
                    var d = v as Dictionary<string, object>;
                    var tt = statement.ParamInfo.GetParameterTypeByParamName(k);
                    var pk = tt.GetPrimaryKey();

                    if (!resultDict.TryGetValue(k, out var dd))
                    {
                        dd = new Dictionary<object, IDictionary<string, object>>();
                        resultDict.Add(k, dd);
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
                return new List<object>();
            }

            foreach (var (k, v) in resultDict)
            {
                var tt = statement.ParamInfo.GetParameterTypeByParamName(k);
                var objs = resultDict[k].Select(x =>
                {
                    var v = x.Value;
                    v["__object"] = PopulateObject(tt, v.Keys, v.Values);
                    return v;
                });
                var primaryKeyValue = tt.GetProperty(StringUtils.ToCamel(t.GetPrimaryKey(), true));
                resultObjects.Add(k, objs.ToDictionary(x => primaryKeyValue.GetValue(x["__object"]), x => x));
            }

            BuildBindings(statement, resultObjects);

            return (IEnumerable<object>)typeof(Enumerable)
                            .GetMethod("Cast")
                            .MakeGenericMethod(t)
                            .Invoke(null, new object[] { resultObjects[statement.ParamInfo.GetDefaultParamName()].Values.Select(x => x["__object"]) });
        }

        private void BuildBindings(SqlSelectStatement statement, IDictionary<string, IDictionary<dynamic, IDictionary<string, object>>> resultObjects)
        {
            Func<string, Action<dynamic, dynamic>> propertySetter = (string prop) => (dynamic x, dynamic value) =>
            {
                x.GetType().GetProperty(StringUtils.ToCamel(prop, true), BindingFlags.InvokeMethod)?.SetValue(x, value);
            };
            Action<dynamic, string, dynamic> memberUpdater = (x, memb, value) =>
            {
                propertySetter(memb)(x["__object"], value);
            };

            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) =>
            {
                return x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
            };
            Func<string, Func<dynamic, dynamic>> memberAccessor = (string memb) => (dynamic x) =>
            {
                if (x.ContainsKey(memb))
                {
                    return x[memb];
                }
                return propertyGetter(memb)(x["__object"]);
            };

            Func<Type, IEnumerable<object>, IEnumerable<object>> regularCollection
                = (t, objs) => (IEnumerable<object>)typeof(Enumerable)
                    .GetMethod("Cast")
                    .MakeGenericMethod((t.IsGenericType) ? t.GetGenericArguments()[0] : t)
                    .Invoke(null, new object[] { objs });

            foreach (var bindingSpec in statement.GetAllBindings())
            {
                var fromObjs = resultObjects[bindingSpec.PrimaryParamName].Values;

                Func<dynamic, Func<dynamic, bool>> filteringConditions = (x) => (y) => bindingSpec.ConditionFunc(x, y);
                Func<IDictionary<string, object>, object> fmap = x =>
                {
                    var pkv = memberAccessor(bindingSpec.PrimaryKey)(x);
                    var targetToObjs = resultObjects[bindingSpec.ForeignParamName].Values.Where(filteringConditions(x));
                    if (targetToObjs.Count() > 0)
                    {
                        foreach (var o in targetToObjs)
                        {
                            var k0 = StringUtils.ToCamel(bindingSpec.ForeignKey.Replace("_id", ""), true);
                            memberUpdater(o, k0, x["__object"]);
                        }
                        memberUpdater(x, bindingSpec.MemberInfo.Name, regularCollection((bindingSpec.MemberInfo as PropertyInfo).PropertyType, targetToObjs.Select(y => y["__object"])));
                    }
                    else
                    {
                        memberUpdater(x, bindingSpec.MemberInfo.Name, regularCollection((bindingSpec.MemberInfo as PropertyInfo).PropertyType, MakeGenerator(bindingSpec, x["__object"])));
                    }
                    return x;
                };
                var r = fromObjs.Select(fmap).ToList(); // just do it
                Console.WriteLine(r);
            }

        }
        private IEnumerable<object> MakeGenerator(BindingSpec bindingSpec, object self)
        {
            // yield return null;
            Console.WriteLine(self);
            yield break;
        }

    }
/*
    public class ObjectRepository<T> : ObjectRepository<T, object> where T: class, new()
    {
        public ObjectRepository(Connection conn, SqlFormatter formatter, SqlCollator collator) : base(conn, formatter, collator) { }
    }
*/

    public class ObjectRepository : ObjectRepositoryBase
    {
        // private SqlBuilder<T> _sqlBuilder;

        public ObjectRepository(Connection conn, SqlFormatter formatter, SqlCollator collator)
        {
            _connection = conn;
            // _sqlBuilder = builder;
            _formatter = formatter;
            _collator = collator;

            _connection.Open();
        }

        public IEnumerable<T> Load<T>(Func<SelectStatement<T>, SelectStatement<T>> f) where T : class, new()
        {
            var statement = f(new SelectStatement<T>());
            return Enumerable.Cast<T>(LoadObjects(typeof(T), statement));
        }

        public IEnumerable<T> Load<T>(Expression<Func<T, bool>> f = null) where T : class, new()
        {
            var statement = new SelectStatement<T>();
            if (f != null)
            {
                statement.Where(f);
            }

            return Enumerable.Cast<T>(LoadObjects(typeof(T), statement));
        }

        public IEnumerable<T> Load<T>(params object[] id) where T : class, new()
        {
            var primaryKey = typeof(T).GetPrimaryKey();

            var statement = new SelectStatement<T>();
            statement.Where(primaryKey, id);

            return Enumerable.Cast<T>(LoadObjects(typeof(T), statement));
        }

        public bool Store<T>(T obj) where T : class, new()
        {
            return Store(new T[] { obj });
        }

        public bool Store<T>(IEnumerable<T> objs) where T : class, new()
        {
            return StoreObjects(typeof(T), objs, null);
        }

        public bool Delete<T>(Expression<Func<T, bool>> f) where T : class, new()
        {
            var statement = new DeleteStatement<T>();
            statement.Where(f);

            var (sql, parameters) = _formatter.Format(statement, out var context);
            var rset = _connection.Execute(sql, parameters, context);

            return true;
        }

        public bool Delete<T>(params object[] id) where T : class, new()
        {
            var primaryKey = typeof(T).GetPrimaryKey();

            var statement = new DeleteStatement<T>();
            statement.Where(primaryKey, id);

            var (sql, parameters) = _formatter.Format(statement, out var context);
            var rset = _connection.Execute(sql, parameters, context);

            return true;
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
    }

    public static class PersistentObjectExtensions
    {
        public static bool Save(this object obj)
        {
            throw new NotImplementedException();
        }
    }
}
