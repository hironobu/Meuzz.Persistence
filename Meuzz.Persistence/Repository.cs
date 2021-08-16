#nullable enable

using Meuzz.Foundation;
using Meuzz.Persistence.Core;
using Meuzz.Persistence.Sql;
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
        protected IEnumerable<object> LoadObjects(IStorageContext context, Type t, SqlSelectStatement statement, Action<IEnumerable<object>>? propertySetter = null)
        {
            var rset = context.Execute(statement);
            if (rset != null)
            {
                var results = PopulateObjects(context, t, rset, statement);
                if (propertySetter != null)
                {
                    propertySetter(results);
                }

                foreach (var o in results)
                {
                    yield return o;
                }
            }
            yield break;
        }

        protected IEnumerable<object> MakeDefaultReverseLoader(IStorageContext context, object value, Type targetType)
        {
            var statement = new SqlSelectStatement(targetType);
            statement.BuildCondition(targetType.GetPrimaryKey(), value);

            return LoadObjects(context, targetType, statement);
        }

        protected IEnumerable<object> MakeDefaultLoader(IStorageContext context, object obj, ClassInfoManager.RelationInfoEntry reli)
        {
            var statement = new SqlSelectStatement(reli.TargetType);
            statement.BuildCondition(reli.ForeignKey, obj.GetType().GetPrimaryValue(obj));

            return LoadObjects(context, reli.TargetType, statement, (results) =>
            {
                var t = reli.TargetType;
                var conv = typeof(Enumerable)
                    .GetMethod("Cast")!
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

        protected object PopulateObject(IStorageContext context, Type t, IEnumerable<string> cols, IEnumerable<object?> vals)
        {
            Func<PropertyInfo, object, MemberAssignment> mapper = (k, v) =>
            {
                return Expression.Bind(k, Expression.Constant(Convert.ChangeType(v, k.PropertyType)));
            };
            var bindings = new List<MemberAssignment>();

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
                    if (v != null)
                    {
                        var loader = MakeDefaultReverseLoader(context, v, prop.PropertyType);
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
                        .GetMethod("Cast")!
                        .MakeGenericMethod((tt.IsGenericType) ? tt.GetGenericArguments()[0] : tt);

                    prop.SetValue(obj, conv.Invoke(null, new object[] { MakeDefaultLoader(context, obj, reli) }));
                }
            }

            foreach (var (prop, proploader) in reverseLoaders)
            {
                var loaderField = obj.GetType().GetField($"__load_{prop.Name}", BindingFlags.NonPublic | BindingFlags.Instance);

                var tt = prop.PropertyType;
                var conv = typeof(Enumerable)
                    .GetMethod("Cast")!
                    .MakeGenericMethod((tt.IsGenericType) ? tt.GetGenericArguments()[0] : tt);

                loaderField.SetValue(obj, conv.Invoke(null, new object[] { proploader }));
            }

            PersistableState.Generate(obj); // for reset
            return obj;
        }

        protected bool StoreObjects(IStorageContext context, Type t, IEnumerable<object> objs, IDictionary<string, object?>? extraData)
        {
            var updated = objs.Where(x => t.GetPrimaryValue(x) != null).ToList();
            var inserted = objs.Where(x => t.GetPrimaryValue(x) == null).ToList();

            if (inserted.Count() > 0)
            {
                var tt = typeof(InsertStatement<>).MakeGenericType(t);
                var insertStatement = (SqlInsertStatement)Convert.ChangeType(Activator.CreateInstance(tt), tt)!;
                insertStatement.Append(inserted);
                insertStatement.ExtraData = extraData;
                var rset = context.Execute(insertStatement);

                var pkey = t.GetPrimaryKey();
                var prop = t.GetProperty(StringUtils.ToCamel(pkey, true));
                if (prop == null)
                {
                    throw new InvalidOperationException();
                }
                var classinfo = t.GetClassInfo();

                var results = rset!.Results;
                int newPrimaryId = (int)Convert.ChangeType(results.First()["id"], prop.PropertyType)!;

                foreach (var (y, i) in inserted.Select((x, i) => (x, i)))
                {
                    prop.SetValue(y, newPrimaryId + i);

                    foreach (var rel in classinfo.Relations)
                    {
                        var foreignType = rel.TargetType;
                        var childObjs = rel.PropertyInfo.GetValue(y) as IEnumerable<object>;

                        if (childObjs != null)
                        {
                            StoreObjects(context, foreignType, childObjs, new Dictionary<string, object?>() { { rel.ForeignKey, newPrimaryId } });
                        }
                    }
                }
            }

            if (updated.Count() > 0)
            {
                var tt = typeof(UpdateStatement<>).MakeGenericType(t);
                var updateStatement = (SqlUpdateStatement)Convert.ChangeType(Activator.CreateInstance(tt), tt)!;
                updateStatement.Append(updated);
                context.Execute(updateStatement);
            }

            return true;
        }

        protected IEnumerable<object> PopulateObjects(IStorageContext context, Type t, ResultSet rset, SqlSelectStatement statement)
        {
            var rows = rset.Results.Select(x =>
            {
                var kvs = x.Select(c => (c.Key.Split('.'), c.Value));
                var d = new Dictionary<string, object?>();
                foreach (var (kk, v) in kvs)
                {
                    var dx = d;
                    var k = string.Join('.', kk.Take(kk.Length - 1));
                    {
                        var dx0 = dx;
                        if (dx0.TryGetValue(k, out var value) && value != null)
                        {
                            dx = (Dictionary<string, object?>)value;
                        }
                        else
                        {
                            dx = new Dictionary<string, object?>();
                            dx0[k] = dx;
                        }
                    }
                    dx[kk.Last().ToLower()] = v;
                }

                return d;
            });

            var resultDict = new Dictionary<string, IDictionary<dynamic, IDictionary<string, object?>>>();
            var resultObjects = new Dictionary<string, IDictionary<dynamic, IDictionary<string, object?>>>();

            foreach (var row in rows)
            {
                foreach (var (k, v) in row)
                {
                    var d = (Dictionary<string, object?>)v!;
                    var tt = statement.ParamInfo.GetParameterTypeByParamName(k);
                    var pk = tt.GetPrimaryKey();

                    if (!resultDict.TryGetValue(k, out var dd) || dd == null)
                    {
                        dd = new Dictionary<object, IDictionary<string, object?>>();
                        resultDict[k] = dd;
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
                return new object[] { };
            }

            foreach (var (k, v) in resultDict)
            {
                var tt = statement.ParamInfo.GetParameterTypeByParamName(k);
                var objs = resultDict[k].Select(x =>
                {
                    var v = x.Value;
                    v["__object"] = PopulateObject(context, tt, v.Keys, v.Values);
                    return v;
                });
                var primaryKeyValue = tt.GetProperty(StringUtils.ToCamel(t.GetPrimaryKey(), true))!;
                resultObjects.Add(k!, objs.ToDictionary(x => primaryKeyValue.GetValue(x["__object"])!, x => x));
            }

            BuildBindings(statement, resultObjects);

            return (IEnumerable<object>)typeof(Enumerable)
                            .GetMethod("Cast")!
                            .MakeGenericMethod(t)
                            .Invoke(null, new object[] { resultObjects[statement.ParamInfo.GetDefaultParamName()!].Values.Select(x => x["__object"]) })!;
        }

        private void BuildBindings(SqlSelectStatement statement, IDictionary<string, IDictionary<dynamic, IDictionary<string, object?>>> resultObjects)
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
                    .GetMethod("Cast")!
                    .MakeGenericMethod((t.IsGenericType) ? t.GetGenericArguments()[0] : t)
                    .Invoke(null, new object[] { objs })!;

            foreach (var joiningSpec in statement.RelationSpecs)
            {
                var fromObjs = resultObjects[joiningSpec.Primary.Name].Values;

                Func<dynamic, Func<dynamic, bool>> filteringConditions = (x) => (y) => joiningSpec.ConditionFunc(x, y);
                Func<IDictionary<string, object?>, object> fmap = x =>
                {
                    var pkv = memberAccessor(joiningSpec.PrimaryKey)(x);
                    var targetToObjs = resultObjects[joiningSpec.Foreign.Name].Values.Where(filteringConditions(x));
                    if (targetToObjs.Count() > 0)
                    {
                        foreach (var o in targetToObjs)
                        {
                            var k0 = StringUtils.ToCamel(joiningSpec.ForeignKey.Replace("_id", ""), true);
                            memberUpdater(o, k0, x["__object"]);
                        }
                        memberUpdater(x, joiningSpec.MemberInfo.Name, regularCollection(((PropertyInfo)joiningSpec.MemberInfo).PropertyType, targetToObjs.Select(y => y["__object"])));
                    }
                    else
                    {
                        memberUpdater(x, joiningSpec.MemberInfo.Name, regularCollection(((PropertyInfo)joiningSpec.MemberInfo).PropertyType, MakeGenerator(joiningSpec, x["__object"])));
                    }
                    return x;
                };

                var r = fromObjs.Select(fmap).ToList(); // just do it
                // Console.WriteLine(r);
            }
        }

        private IEnumerable<object> MakeGenerator(RelationSpec bindingSpec, object? self)
        {
            // yield return null;
            Console.WriteLine(self);
            yield break;
        }
    }

    public class ObjectRepository<T> : ObjectRepository where T : class
    {
        public ObjectRepository() : base() { }

        public IEnumerable<T> Load(IStorageContext context, Func<SelectStatement<T>, SelectStatement<T>> f) => Load<T>(context, f);

        public IEnumerable<T2> Load<T2>(IStorageContext context, Func<SelectStatement<T>, SelectStatement<T2>> f) where T2 : class => Load<T, T2>(context, f);

        public IEnumerable<T> Load(IStorageContext context, Expression<Func<T, bool>> f) => Load<T>(context, f);

        public IEnumerable<T> Load(IStorageContext context, params object[] id) => Load<T>(context, id);

        public bool Store(IStorageContext context, params T[] objs) => Store<T>(context, objs);

        public bool Delete(IStorageContext context, Expression<Func<T, bool>> f) => Delete<T>(context, f);

        public bool Delete(IStorageContext context, params object[] id) => Delete<T>(context, id);
    }


    public class ObjectRepository : ObjectRepositoryBase
    {
        public ObjectRepository()
        {
        }

        public IEnumerable<T> Load<T>(IStorageContext context, Func<SelectStatement<T>, SelectStatement<T>> f) where T : class
        {
            return Load<T, T>(context, f);
        }

        public IEnumerable<T2> Load<T, T2>(IStorageContext context, Func<SelectStatement<T>, SelectStatement<T2>> f) where T : class where T2 : class
        {
            var statement = f(new SelectStatement<T>());
            return Enumerable.Cast<T2>(LoadObjects(context, typeof(T2), statement));
        }

        public IEnumerable<T> Load<T>(IStorageContext context, Expression<Func<T, bool>> f) where T : class
        {
            var statement = new SelectStatement<T>();
            if (f != null)
            {
                statement.Where(f);
            }

            return Enumerable.Cast<T>(LoadObjects(context, typeof(T), statement));
        }

        public IEnumerable<T> Load<T>(IStorageContext context, params object[] id) where T : class
        {
            var primaryKey = typeof(T).GetPrimaryKey();

            var statement = new SelectStatement<T>();
            statement.Where(primaryKey, id);

            return Enumerable.Cast<T>(LoadObjects(context, typeof(T), statement));
        }

        public bool Store<T>(IStorageContext context, params T[] objs) where T : class
        {
            return StoreObjects(context, typeof(T), objs, null);
        }

        public bool Delete<T>(IStorageContext context, Expression<Func<T, bool>> f) where T : class
        {
            var statement = new DeleteStatement<T>();
            statement.Where(f);

            context.Execute(statement);

            return true;
        }

        public bool Delete<T>(IStorageContext context, params object[] id) where T : class
        {
            var primaryKey = typeof(T).GetPrimaryKey();

            var statement = new DeleteStatement<T>();
            statement.Where(primaryKey, id);

            context.Execute(statement);

            return true;
        }

        /*
        public class MyEqualityComparer : IEqualityComparer<object>
        {
            private bool IsNumeric(object? x)
            {
                return x is int || x is long || x is uint || x is ulong;
            }

            public new bool Equals(object? x, object? y)
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
        }*/
    }

    public static class PersistentObjectExtensions
    {
        public static bool Save(this object obj)
        {
            throw new NotImplementedException();
        }
    }
}
