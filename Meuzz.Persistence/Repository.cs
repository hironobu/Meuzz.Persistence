using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class GeneratorUtils
    {
        public static IEnumerable<object> MakeGenerator(ClassInfoManager.RelationInfoEntry reli)
        {
            Console.WriteLine(reli);
            yield break;
        }


    }

    public class ObjectRepositoryBase
    {
        protected Connection _connection = null;

        protected SqlFormatter _formatter;
        protected SqlCollator _collator;

        protected object PopulateObject(Type t, IEnumerable<string> cols, IEnumerable<object> vals)
        {
            Func<PropertyInfo, object, MemberAssignment> mapper = (k, v) =>
            {
                return Expression.Bind(k, Expression.Constant(Convert.ChangeType(v, k.PropertyType)));
            };
            var bindings = new List<MemberAssignment>();

            foreach (var (c, v) in cols.Zip(vals))
            {
                var prop = t.GetPropertyInfoFromColumnName(c, true);
                if (prop == null)
                {
                    continue;
                }

                if (prop.PropertyType.IsPersistent())
                {
                    bindings.Add(mapper(prop, null));
                }
                else
                {
                    bindings.Add(mapper(prop, v));
                }
            };

            // var props = cols.Select(c => t.GetPropertyInfoFromColumnName(c)).Where(x => x != null).ToList<PropertyInfo>();
            // var valss = vals.ToList();
            var ci = t.GetClassInfo();

            foreach (var reli in ci.Relations)
            {
                var prop = reli.PropertyInfo;
                if (prop != null)
                {
                    var tt = prop.PropertyType;

                    var conv = typeof(Enumerable)
                        .GetMethod("Cast")
                        .MakeGenericMethod((tt.IsGenericType) ? tt.GetGenericArguments()[0] : tt);
                    var ffs = typeof(GeneratorUtils).GetMethods();
                        var ff = ffs.Where(x => x.Name == "MakeGenerator").First();
                    bindings.Add(
                        Expression.Bind(prop,
                            Expression.Call(null, conv,
                                Expression.Call(null, ff, Expression.Constant(reli))
                                )
                        ));
                }
            }

            // var bindings = props.Zip(valss, );

            NewExpression instance = Expression.New(t);
            Expression expr = Expression.MemberInit(instance, bindings);

            var ft = typeof(Func<>).MakeGenericType(t);
            LambdaExpression lambda = Expression.Lambda(ft, expr);
            dynamic func = Convert.ChangeType(lambda.Compile(), ft);
            return func();
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
                        var foreignType = rel.TargetClassType;
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
    }

    public class ObjectRepository<T> : ObjectRepository<T, object> where T: class, new()
    {
        public ObjectRepository(Connection conn, SqlBuilder<T> builder, SqlFormatter formatter, SqlCollator collator) : base(conn, builder, formatter, collator) { }
    }

    public class ObjectRepository<T, I> : ObjectRepositoryBase where T : class, new()
    {
        private SqlBuilder<T> _sqlBuilder;

        public ObjectRepository(Connection conn, SqlBuilder<T> builder, SqlFormatter formatter, SqlCollator collator)
        {
            _connection = conn;
            _sqlBuilder = builder;
            _formatter = formatter;
            _collator = collator;

            _connection.Open();
        }

        public StatementProcessor<T> Load(Func<SelectStatement<T>, SelectStatement<T>> f)
        {
            var statement = f(new SelectStatement<T>());
            return new StatementProcessor<T>()
            {
                Statement = statement,
                OnExecute = (stmt) =>
                {
                    var (sql, parameters) = _formatter.Format(stmt, out var context);
                    var rset = _connection.Execute(sql, parameters, context);
                    return PopulateObjects(rset, stmt, context);
                }
            };
        }

        public StatementProcessor<T> Load(Expression<Func<T, bool>> f = null)
        {
            var statement = new SelectStatement<T>();
            if (f != null)
            {
                statement.Where(f);
            }

            return new StatementProcessor<T>()
            {
                Statement = statement,
                OnExecute = (stmt) =>
                {
                    var (sql, parameters) = _formatter.Format(stmt, out var context);
                    var rset = _connection.Execute(sql, parameters, context);
                    return PopulateObjects(rset, stmt, context);
                }
            };
        }

        public StatementProcessor<T> Load(params object[] id)
        {
            var primaryKey = typeof(T).GetPrimaryKey();

            var statement = new SelectStatement<T>();
            statement.Where(primaryKey, id);

            return new StatementProcessor<T>()
            {
                Statement = statement,
                OnExecute = (stmt) =>
                {
                    var (sql, parameters) = _formatter.Format(stmt, out var context);
                    var rset = _connection.Execute(sql, parameters, context);
                    return PopulateObjects(rset, stmt, context);
                }
            };
        }


        public bool Store(T obj)
        {
            return Store(new T[] { obj });
        }

        public bool Store(IEnumerable<T> objs)
        {
            return StoreObjects(typeof(T), objs, null);
        }

        public bool Delete(Expression<Func<T, bool>> f)
        {
            var statement = new DeleteStatement<T>();
            statement.Where(f);

            var (sql, parameters) = _formatter.Format(statement, out var context);
            var rset = _connection.Execute(sql, parameters, context);

            return true;
        }

        public bool Delete(params object[] id)
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
                return new List<T>();
            }

            foreach (var (k, v) in resultDict) {
                var t = statement.ParamInfo.GetParameterTypeByParamName(k);
                var objs = resultDict[k].Select(x =>
                {
                    var v = x.Value;
                    v["__object"] = PopulateObject(t, v.Keys, v.Values);
                    return v;
                });
                var primaryKeyValue = t.GetProperty(StringUtils.ToCamel(t.GetPrimaryKey(), true));
                resultObjects.Add(k, objs.ToDictionary(x => primaryKeyValue.GetValue(x["__object"]), x => x));
            }

            BuildBindings(statement, resultObjects);

            return (IEnumerable<T>)typeof(Enumerable)
                            .GetMethod("Cast")
                            .MakeGenericMethod(typeof(T))
                            .Invoke(null, new object[] { resultObjects[statement.ParamInfo.GetDefaultParamName()].Values.Select(x => x["__object"]) });
        }

        private void BuildBindings(SqlSelectStatement statement, IDictionary<string, IDictionary<dynamic, IDictionary<string, object>>> resultObjects)
        {
            Func<string, Action<dynamic, dynamic>> propertySetter = (string prop) => (dynamic x, dynamic value) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true))?.SetValue(x, value);
            Action<dynamic, string, dynamic> memberUpdater = (x, memb, value) =>
            {
                propertySetter(memb)(x["__object"], value);
            };

            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
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

    //     public class ObjectRepository<T>  : ObjectRepository<T, int>  where T new() { }

    public static class PersistentObjectExtensions
    {
        public static bool Save(this object obj)
        {
            throw new NotImplementedException();
        }
    }
}
