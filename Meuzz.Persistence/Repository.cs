﻿#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;
using Meuzz.Persistence.Sql;

namespace Meuzz.Persistence
{
    public class ObjectRepositoryBase
    {
        protected IEnumerable<object> LoadObjects(IDatabaseContext context, Type t, SqlSelectStatement statement, Action<IEnumerable<object>>? propertySetter = null)
        {
            var rset = context.Execute(statement);
            if (rset != null)
            {
                var results = (IEnumerable<object>)PopulateObjects(context, t, rset, statement);
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

        protected IEnumerable<object> MakeDefaultReverseLoader(IDatabaseContext context, object value, Type targetType)
        {
            var primaryKey = targetType.GetPrimaryKey();
            if (primaryKey == null) { throw new ArgumentException("Argument type should be persistent", "targetType"); }

            var statement = new SqlSelectStatement(targetType);
            statement.BuildCondition(primaryKey, value);

            return LoadObjects(context, targetType, statement);
        }

        protected IEnumerable<object> MakeDefaultLoader(IDatabaseContext context, object obj, TableInfoManager.RelationInfoEntry reli)
        {
            var statement = new SqlSelectStatement(reli.TargetType);
            var pkval = obj.GetType().GetPrimaryValue(obj);
            if (pkval == null) { throw new NotImplementedException(); }
            statement.BuildCondition(reli.ForeignKey, pkval);

            return LoadObjects(context, reli.TargetType, statement, (results) =>
            {
                if (reli.InversePropertyInfo != null)
                {
                    foreach (var x in results)
                    {
                        var iprop = reli.InversePropertyInfo;
                        SetPropertyValue(iprop, x, obj);
                    }
                }
                var prop = reli.PropertyInfo;
                SetPropertyValue(prop, obj, EnumerableCast(reli.TargetType, results));
            });
        }

        protected object PopulateObject(IDatabaseContext context, Type t, IEnumerable<string> columns, IEnumerable<object?> values)
        {
            Func<PropertyInfo, object?, MemberAssignment> mapper = (k, v) => Expression.Bind(k, Expression.Constant(Convert.ChangeType(v, k.PropertyType)));
            var bindings = new List<MemberAssignment>();
            var arguments = new List<Expression>();

            IDictionary<PropertyInfo, IEnumerable<object>> reverseLoaders = new Dictionary<PropertyInfo, IEnumerable<object>>();

            var ctors = t.GetConstructors().OrderBy(x => x.GetParameters().Length);
            var ctor = ctors.First();
            var ctorParamTypesAndNames = ctor.GetParameters().Select(x => (x.ParameterType, x.Name.ToSnake())).ToArray();

            var colsValsPairs = columns.Zip(values);
            var ctorParamsDict = colsValsPairs.Where(x => ctorParamTypesAndNames.Any(p => p.Item2 == x.First)).ToDictionary(x => x.First, x => x.Second);
            var memberParamsDict = colsValsPairs.Where(x => !ctorParamTypesAndNames.Any(p => p.Item2 == x.First)).ToDictionary(x => x.First, x => x.Second);

            foreach (var (pt, n) in ctorParamTypesAndNames)
            {
                switch (pt)
                {
                    case Type intType when intType == typeof(int):
                        arguments.Add(Expression.Constant(Convert.ToInt32(ctorParamsDict[n])));
                        break;

                    default:
                        arguments.Add(Expression.Constant(ctorParamsDict[n]));
                        break;
                }
            }

            foreach (var (c, v) in memberParamsDict)
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

            NewExpression instance = Expression.New(ctor, arguments);
            Expression expr = Expression.MemberInit(instance, bindings);

            var ft = typeof(Func<>).MakeGenericType(t);
            LambdaExpression lambda = Expression.Lambda(ft, expr);
            dynamic func = Convert.ChangeType(lambda.Compile(), ft);
            object obj = func();

            var ci = t.GetTableInfo();
            if (ci == null) { throw new NotImplementedException(); }

            foreach (var reli in ci.Relations)
            {
                var prop = reli.PropertyInfo;
                if (prop != null)
                {
                    SetPropertyValue(prop, obj, EnumerableCast(prop.PropertyType, MakeDefaultLoader(context, obj, reli)));
                }
            }

            foreach (var (prop, proploader) in reverseLoaders)
            {
                var loaderField = obj.GetType().GetField($"__load_{prop.Name}", BindingFlags.NonPublic | BindingFlags.Instance);
                if (loaderField != null)
                {
                    loaderField.SetValue(obj, EnumerableCast(prop.PropertyType, proploader));
                }
            }

            PersistableState.Generate(obj); // for reset
            return obj;
        }

        protected bool StoreObjects(IDatabaseContext context, Type t, IEnumerable<object> objs, IDictionary<string, object?>? extraData)
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
                if (pkey == null) { throw new NotImplementedException(); }
                var prop = t.GetProperty(pkey.ToCamel(true))!;
                var classinfo = t.GetTableInfo();
                if (classinfo == null) { throw new NotImplementedException(); }

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

        protected IEnumerable<object> PopulateObjects(IDatabaseContext context, Type t, ResultSet rset, SqlSelectStatement statement)
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

            var resultDicts = new Dictionary<string, IList<(string?, object?, IDictionary<string, object?>)>>();
            var resultObjects = new Dictionary<string, IDictionary<object, IDictionary<string, object?>>>();
            var resultDefaultObjects = new Dictionary<string, IEnumerable<IDictionary<string, object?>>>();

            foreach (var row in rows)
            {
                foreach (var (k, v) in row)
                {
                    var d = (Dictionary<string, object?>)v!;
                    var tt = statement.ParameterSetInfo.GetTypeByName(k) ?? statement.OutputType;
                    var pk = tt.GetPrimaryKey();

                    if (!resultDicts.TryGetValue(k, out var dd) || dd == null)
                    {
                        dd = new List<(string?, object?, IDictionary<string, object?>)>();
                        resultDicts[k] = dd;
                    }

                    var pkval = pk != null && d.ContainsKey(pk) ? d[pk] : null;
                    dd.Add((pk, pkval, d));
                }
            }

            if (!resultDicts.Any())
            {
                return Enumerable.Empty<object>();
            }

            foreach (var (k, v) in resultDicts)
            {
                if (!resultObjects.ContainsKey(k!))
                {
                    var tt = statement.ParameterSetInfo.GetTypeByName(k);
                    var objs = v.Select(x =>
                    {
                        var xv = x.Item3;
                        if (tt != null || statement.OutputSpec == null)
                        {
                            xv["__object"] = x.Item2 != null ? PopulateObject(context, tt ?? statement.Type, xv.Keys, xv.Values) : null;
                        }
                        else
                        {
                            Func<IDictionary<string, object?>, object?> f = (Func<IDictionary<string, object?>, object?>)statement.OutputSpec.OutputExpression.Compile();
                            xv["__object"] = f(x.Item3);
                        }
                        return (x.Item2, xv);
                    });
                    resultObjects[k!] = objs.Where(x => x.Item1 != null).GroupBy(x => x.Item1, x => x.Item2).ToDictionary(x => x.Key!, x => x.First());
                    resultDefaultObjects[k!] = objs.Where(x => x.Item1 == null).Select(x => x.Item2);
                }
            }

            BuildBindings(statement, resultObjects);

            var objects = resultObjects[statement.ParameterSetInfo.GetDefaultParamName()!].Values;
            if (!objects.Any())
            {
                objects = resultDefaultObjects[statement.ParameterSetInfo.GetDefaultParamName()!].ToArray();
            }
            var rets = EnumerableCast(t, objects.Select(x => x["__object"]));
            return (IEnumerable<object>)rets;
        }

        private void BuildBindings(SqlSelectStatement statement, IDictionary<string, IDictionary<dynamic, IDictionary<string, object?>>> resultObjects)
        {
            Func<string, Action<object?, object?>> propertySetter = (string prop) => (object? x, object? value) =>
            {
                if (x != null)
                {
                    x.GetType().GetProperty(prop.ToCamel(true), BindingFlags.InvokeMethod)?.SetValue(x, value);
                }
            };
            Action<IDictionary<string, object?>, string, object?> memberUpdater = (x, memb, value) =>
            {
                if (x != null)
                {
                    propertySetter(memb)(x["__object"], value);
                }
            };

            Func<string, Func<object?, object?>> propertyGetter = (string prop) => (object? x) =>
            {
                return x != null ? x.GetType().GetProperty(prop.ToCamel(true))!.GetValue(x) : null;
            };
            Func<string, Func<IDictionary<string, object?>, object?>> memberAccessor = (string memb) => (IDictionary<string, object?> x) =>
            {
                if (x.ContainsKey(memb))
                {
                    return x[memb];
                }
                return propertyGetter(memb)(x["__object"]);
            };

            Func<Type, IEnumerable<object>, IEnumerable<object>> regularCollection = (t, objs) => (IEnumerable<object>)EnumerableCast(t, objs)!;

            foreach (var joiningSpec in statement.RelationSpecs)
            {
                var fromObjs = resultObjects[joiningSpec.Left.Name].Values;

                Func<dynamic, Func<dynamic, bool>> filteringConditions = (x) => (y) => joiningSpec.ConditionFunc(x, y);
                Func<IDictionary<string, object?>, object> fmap = x =>
                {
                    var pkv = memberAccessor(joiningSpec.PrimaryKey)(x);
                    var targetToObjs = resultObjects[joiningSpec.Right.Name].Values.Where(filteringConditions(x));
                    if (targetToObjs.Any())
                    {
                        foreach (var o in targetToObjs)
                        {
                            var k0 = joiningSpec.ForeignKey.Replace("_id", "").ToCamel(true);
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

        private void SetPropertyValue(PropertyInfo propInfo, object obj, object value)
        {
            if (propInfo.SetMethod != null)
            {
                propInfo.SetValue(obj, value);
            }
            else
            {
                // prop.GetCustomAttribute
                var attr = propInfo.GetCustomAttribute<BackingFieldAttribute>();
                if (attr != null)
                {
                    var field = propInfo.DeclaringType?.GetField(attr.BackingFieldName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                    field?.SetValue(obj, value);
                }
            }
        }

        private IEnumerable<object> MakeGenerator(RelationSpec bindingSpec, object? self)
        {
            // yield return null;
            Console.WriteLine(self);
            yield break;
        }

        private object EnumerableCast(Type t, IEnumerable<object?> args)
        {
            // TODO: array or list以外は避けるように(ex. IDictionary<>)
            var t1 = t.IsGenericType && t.GetGenericArguments().Length == 1 ? t.GetGenericArguments()[0] : t;

            switch (t1)
            {
                case Type inttype when inttype == typeof(int):
                    return args.Select(x => (object)Convert.ToInt32(x));

                case Type longtype when longtype == typeof(long):
                    return args.Select(x => (object)Convert.ToInt64(x));

                default:
                    var conv = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(t1);
                    return conv.Invoke(null, new[] { args })!;
            }
        }
    }

    public class ObjectRepository<T> : ObjectRepository where T : class
    {
        public ObjectRepository() : base() { }

        public IEnumerable<T> Load(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T>> f) => Load<T>(context, f);

        public IEnumerable<T2> Load<T2>(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T2>> f) => Load<T, T2>(context, f);

        public IEnumerable<T> Load(IDatabaseContext context, Expression<Func<T, bool>> f) => Load<T>(context, f);

        public IEnumerable<T> Load(IDatabaseContext context, params object[] id) => Load<T>(context, id);

        public bool Store(IDatabaseContext context, params T[] objs) => Store<T>(context, objs);

        public bool Delete(IDatabaseContext context, Expression<Func<T, bool>> f) => Delete<T>(context, f);

        public bool Delete(IDatabaseContext context, params object[] id) => Delete<T>(context, id);
    }


    public class ObjectRepository : ObjectRepositoryBase
    {
        public ObjectRepository()
        {
        }

        public IEnumerable<T> Load<T>(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T>> f)
        {
            return Load<T, T>(context, f);
        }

        public IEnumerable<T2> Load<T, T2>(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T2>> f)
        {
            var statement = f(new SelectStatement<T>());
            return Enumerable.Cast<T2>(LoadObjects(context, typeof(T2), statement));
        }

        public IEnumerable<T> Load<T>(IDatabaseContext context, Expression<Func<T, bool>> f)
        {
            var statement = new SelectStatement<T>();
            if (f != null)
            {
                statement = statement.Where(f);
            }

            return Enumerable.Cast<T>(LoadObjects(context, typeof(T), statement));
        }

        public IEnumerable<T> Load<T>(IDatabaseContext context, params object[] id)
        {
            var primaryKey = typeof(T).GetPrimaryKey();
            if (primaryKey == null) { throw new NotSupportedException(); }

            var statement = new SelectStatement<T>();
            statement = statement.Where(primaryKey, id);

            return Enumerable.Cast<T>(LoadObjects(context, typeof(T), statement));
        }

        public bool Store<T>(IDatabaseContext context, params T[] objs) where T : class
        {
            return StoreObjects(context, typeof(T), objs, null);
        }

        public bool Delete<T>(IDatabaseContext context, Expression<Func<T, bool>> f)
        {
            var statement = new DeleteStatement<T>();
            statement.Where(f);

            context.Execute(statement);

            return true;
        }

        public bool Delete<T>(IDatabaseContext context, params object[] id)
        {
            var primaryKey = typeof(T).GetPrimaryKey();
            if (primaryKey == null) { throw new NotSupportedException(); }

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
