﻿#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;
using Meuzz.Persistence.Database;
using Meuzz.Persistence.Sql;

namespace Meuzz.Persistence
{
    public class ObjectRepositoryBase
    {
        protected IEnumerable<object> MakeLoader(IDatabaseContext context, Type t, SqlSelectStatement statement)
        {
            var rset = context.Execute(statement);
            if (rset != null)
            {
                var results = PopulateObjects(context, t, rset, statement);
                foreach (var o in results)
                {
                    yield return o;
                }
            }
            yield break;
        }

        protected IEnumerable<object> MakeDefaultLoader(IDatabaseContext context, Type targetType, object value)
        {
            var primaryKey = targetType.GetPrimaryKey();
            if (primaryKey == null) { throw new ArgumentException("Argument type should be persistent", "targetType"); }

            var statement = new SqlSelectStatement(targetType);
            statement.BuildCondition(primaryKey, value);

            return MakeLoader(context, targetType, statement);
        }

        protected IEnumerable<object> MakeDefaultLoader(IDatabaseContext context, object obj, RelationInfo reli)
        {
            SqlSelectStatement statement;

            if (reli.ThroughType != null && reli.ThroughForeignKey != null)
            {
                statement = new SqlSelectStatement(reli.ThroughType);
                var pkval = obj.GetType().GetPrimaryValue(obj);
                if (pkval == null) { throw new NotImplementedException(); }
                statement.BuildCondition(reli.ThroughForeignKey, pkval);

                // statement = new SqlSelectStatement(statement);

                var throughMemberInfo = reli.ThroughType.GetPropertyInfoFromColumnName(reli.ThroughForeignKey);
                if (throughMemberInfo == null) { throw new NotImplementedException();  }
                var targetPrimaryMemberInfo = reli.TargetType.GetPropertyInfoFromColumnName(reli.TargetType.GetPrimaryKey() ?? "id");
                if (targetPrimaryMemberInfo == null) { throw new NotImplementedException(); }
                var cond = ExpressionHelpers.MakeEqualityConditionFunc(throughMemberInfo, targetPrimaryMemberInfo);
                statement.BuildRelationSpec(cond);

                var tupleType = typeof(Tuple<,>).MakeGenericType(reli.ThroughType, reli.TargetType);
                var outputfunc = ExpressionHelpers.MakeUntupleByLastMemberAccessFunc(tupleType);
                statement.BuildOutputSpec(outputfunc);
            }
            else
            {
                statement = new SqlSelectStatement(reli.TargetType);
                var pkval = obj.GetType().GetPrimaryValue(obj);
                if (pkval == null) { throw new NotImplementedException(); }
                statement.BuildCondition(reli.ForeignKey, pkval);
            }

            var results = MakeLoader(context, reli.TargetType, statement);
            
            {
                if (reli.InversePropertyInfo != null)
                {
                    foreach (var x in results)
                    {
                        ReflectionHelpers.PropertyOrFieldSet(x, reli.InversePropertyInfo, obj);
                    }
                }
                ReflectionHelpers.PropertyOrFieldSet(obj, reli.PropertyInfo, results.EnumerableUncast(reli.TargetType));
            }

            return results;
        }

        private static Expression MakeConstantExpression(Type t, object? value)
        {
            switch (t)
            {
                case Type intType when intType == typeof(long):
                    return Expression.Constant(Convert.ToInt64(value));

                case Type intType when intType == typeof(int):
                    return Expression.Constant(Convert.ToInt32(value));

                default:
                    return Expression.Constant(value);
            }
        }


        private static Expression MakePropertyOrFieldSetExpression(Expression exprObj, PropertyInfo propInfo, Func<object, object> valuefunc)
        {
            var exprValueFunc = Expression.Convert(Expression.Invoke(Expression.Constant(valuefunc), exprObj), propInfo.PropertyType);

            if (propInfo.SetMethod != null)
            {
                var memberExpr = Expression.Property(exprObj, propInfo);

                return Expression.Assign(memberExpr, Expression.Convert(Expression.Invoke(Expression.Constant(valuefunc), exprObj), propInfo.PropertyType));
            }
            else
            {
                var attr = propInfo.GetCustomAttribute<BackingFieldAttribute>();
                if (attr == null)
                {
                    throw new NotImplementedException();
                }

                var field = propInfo.DeclaringType?.GetField(attr.BackingFieldName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                Action<object, object> fieldSetter = (obj, value) => field?.SetValue(obj, value);
                return Expression.Invoke(Expression.Constant(fieldSetter), exprObj, exprValueFunc);
            }
        }

        private static Expression? MakeLoaderFieldSetExpression(Expression exprObj, PropertyInfo prop, IEnumerable<object> proploader)
        {
            var loaderField = exprObj.Type.GetField($"__load_{prop.Name}", BindingFlags.NonPublic | BindingFlags.Instance);
            if (loaderField == null)
            {
                return null;
            }

            Action<object> loaderFieldSetter = (obj) => loaderField.SetValue(obj, proploader.EnumerableUncast(prop.PropertyType));
            return Expression.Invoke(Expression.Constant(loaderFieldSetter), exprObj);
        }

        protected object PopulateObject(IDatabaseContext context, Type t, IDictionary<string, object?> valueDict)
        {
            var bindings = new List<MemberAssignment>();
            var reverseLoaders = new Dictionary<PropertyInfo, IEnumerable<object>>();

            var ctor = t.GetConstructors().OrderBy(x => x.GetParameters().Length).First();
            var ctorParamTypesAndNames = ctor.GetParameters().Select(x => (x.ParameterType, x.Name.ToSnake())).ToArray();

            var ctorParamDict = valueDict.Where(x => ctorParamTypesAndNames.Any(p => p.Item2 == x.Key)).ToDictionary(x => x.Key, x => x.Value);
            var memberInitParamDict = valueDict.Where(x => !ctorParamTypesAndNames.Any(p => p.Item2 == x.Key)).ToDictionary(x => x.Key, x => x.Value);

            var arguments = ctorParamTypesAndNames.Select(p => MakeConstantExpression(p.Item1, ctorParamDict[p.Item2]));

            foreach (var (c, v) in memberInitParamDict)
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
                        var loader = MakeDefaultLoader(context, prop.PropertyType, v);
                        reverseLoaders.Add(prop, loader);
                    }
                }
                else
                {
                    Func<PropertyInfo, object?, MemberAssignment> mapper = (k, v) => Expression.Bind(k, Expression.Constant(Convert.ChangeType(v, k.PropertyType)));
                    bindings.Add(mapper(prop, v));
                }
            };

            NewExpression exprNew = Expression.New(ctor, arguments);
            Expression exprMemberInit = Expression.MemberInit(exprNew, bindings);

            var ft = typeof(Func<>).MakeGenericType(t);
            LambdaExpression lambda = Expression.Lambda(ft, exprMemberInit);
            var exprObj = Expression.Variable(t);
            var exprObjAssigned = Expression.Assign(exprObj, exprMemberInit);

            var exprs = new List<Expression>();

            var ci = t.GetTableInfo();
            if (ci == null) { throw new NotImplementedException(); }

            exprs.Add(exprObjAssigned);

            foreach (var reli in ci.Relations)
            {
                var prop = reli.PropertyInfo;
                if (prop != null)
                {
                    var e = MakePropertyOrFieldSetExpression(exprObj, prop, (object obj) => MakeDefaultLoader(context, obj, reli).EnumerableUncast(prop.PropertyType));
                    exprs.Add(e);
                }
            }

            foreach (var (prop, proploader) in reverseLoaders)
            {
                var e = MakeLoaderFieldSetExpression(exprObj, prop, proploader);
                if (e != null)
                {
                    exprs.Add(e);
                }
            }

            exprs.Add(exprObj);

            var func0 = Expression.Lambda(Expression.Block(new[] { exprObj }, exprs)).Compile();

            Func<object> func = (Func<object>)Convert.ChangeType(func0, ft);
            object obj = func();

            PersistableState.Reset(obj);
            return obj;
        }

        protected bool StoreObjects(IDatabaseContext context, Type t, IEnumerable<object> objs, IDictionary<string, object?>? extraData)
        {
            var updated = objs.Where(x => !PersistableState.IsNew(x)).ToList();
            var inserted = objs.Where(x => PersistableState.IsNew(x)).ToList();

            if (inserted.Any())
            {
                var tt = typeof(InsertStatement<>).MakeGenericType(t);
                var insertStatement = (SqlInsertStatement)Convert.ChangeType(Activator.CreateInstance(tt), tt)!;
                insertStatement.Append(inserted);
                insertStatement.ExtraData = extraData;
                var rset = context.Execute(insertStatement);

                var pkey = t.GetPrimaryKey();
                if (pkey == null) { throw new NotImplementedException(); }
                var prop = t.GetProperty(pkey.ToCamel(true))!;
                var tableInfo = t.GetTableInfo();
                if (tableInfo == null) { throw new NotImplementedException(); }

                var results = rset!.Results;
                int newPrimaryId = (int)Convert.ChangeType(results.First()["id"], prop.PropertyType)!;

                foreach (var (y, i) in inserted.Select((x, i) => (x, i)))
                {
                    prop.SetValue(y, newPrimaryId + i);

                    foreach (var rel in tableInfo.Relations)
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

            if (updated.Any())
            {
                var tt = typeof(UpdateStatement<>).MakeGenericType(t);
                var updateStatement = (SqlUpdateStatement)Convert.ChangeType(Activator.CreateInstance(tt), tt)!;
                updateStatement.Append(updated);
                context.Execute(updateStatement);
            }

            return true;
        }

        private object? __PopulateObject(IDatabaseContext context, Type tt, IDictionary<string, object?> d, SqlSelectStatement statement)
        {
            if (tt != null || statement.OutputSpec == null)
            {
                return PopulateObject(context, tt ?? statement.Type, d);
            }
            else
            {
                Func<IDictionary<string, object?>, object?> f = statement.OutputSpec.CompiledOutputFunc;
                return f(d);
            }
        }

        protected IEnumerable<object> PopulateObjects(IDatabaseContext context, Type t, ResultSet rset, SqlSelectStatement statement)
        {
            var rows = rset.Grouped();
            if (!rows.Any())
            {
                return Array.Empty<object>();
            }

            var resultRows = new List<IDictionary<string, IDictionary<string, object?>>>();
            var resultDicts = new Dictionary<string, List<(object?, IDictionary<string, object?>)>>();
            var indexedResultDicts = new Dictionary<string, Dictionary<object, IDictionary<string, object?>>>();

            foreach (var row in rows)
            {
                var rrow = new Dictionary<string, IDictionary<string, object?>>();

                foreach (var (k, v) in row)
                {
                    var d = (IDictionary<string, object?>)v!;
                    var tt = statement.ParameterSetInfo.GetTypeByName(k) ?? statement.OutputType;
                    var pk = tt.GetPrimaryKey();

                    var dd = resultDicts.GetValueOrNew(k);
                    var idd = indexedResultDicts.GetValueOrNew(k);

                    var pkval = pk != null && d.ContainsKey(pk) ? d[pk] : null;
                    if (pkval != null)
                    {
                        if (!idd.ContainsKey(pkval))
                        {
                            d["__object"] = __PopulateObject(context, tt, d, statement);
                            idd.Add(pkval, d);
                        }
                    }
                    else
                    {
                        d["__object"] = null;
                    }

                    dd.Add((pkval, d));
                    rrow[k] = d;
                }

                resultRows.Add(rrow);
            }

            BuildBindings(statement, indexedResultDicts);

            if (!t.IsTuple())
            {
                IEnumerable<IDictionary<string, object?>> objects = indexedResultDicts[statement.ParameterSetInfo.GetDefaultParamName()].Values;
                if (!objects.Any())
                {
                    objects = resultDicts[statement.ParameterSetInfo.GetDefaultParamName()].Select(x => x.Item2).ToArray();
                }
                return (IEnumerable<object>)objects.Select(x => x["__object"]).EnumerableUncast(t);
            }
            else
            {
                if (statement.PackerFunc == null)
                {
                    throw new NotImplementedException();
                }

                return resultRows.Select(x => TypedTuple.Make(statement.PackerFunc(x)));
            }
        }

        private void BuildBindings(SqlSelectStatement statement, Dictionary<string, Dictionary<object, IDictionary<string, object?>>> resultObjects)
        {
            foreach (var relationSpec in statement.RelationSpecs)
            {
                if (relationSpec.MemberInfo == null)
                {
                    continue;
                }

                var memberType = relationSpec.MemberInfo.GetMemberType();

                foreach (var dx in resultObjects[relationSpec.Left.Name].Values)
                {
                    var targetToObjs = resultObjects[relationSpec.Right.Name].Values.Where(y => relationSpec.ConditionFunc(dx, y));
                    if (targetToObjs.Any())
                    {
                        var inversePropertyName = relationSpec.ForeignKey.Replace("_id", "").ToCamel(true);
                        foreach (var o in targetToObjs)
                        {
                            ReflectionHelpers.PropertySet(o, inversePropertyName, dx["__object"]);
                        }
                        ReflectionHelpers.PropertySet(dx, relationSpec.MemberInfo.Name, targetToObjs.Select(y => y["__object"]).EnumerableUncast(memberType));
                    }
                    else
                    {
                        Func<IEnumerable<object>> dummyLoader = () => Enumerable.Empty<object>();
                        ReflectionHelpers.PropertySet(dx, relationSpec.MemberInfo.Name, dummyLoader);
                    }
                };
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
            return Enumerable.Cast<T2>(MakeLoader(context, typeof(T2), statement));
        }

        public IEnumerable<T> Load<T>(IDatabaseContext context, Expression<Func<T, bool>> f)
        {
            var statement = new SelectStatement<T>();
            if (f != null)
            {
                statement = statement.Where(f);
            }

            return Enumerable.Cast<T>(MakeLoader(context, typeof(T), statement));
        }

        public IEnumerable<T> Load<T>(IDatabaseContext context, params object[] id)
        {
            var primaryKey = typeof(T).GetPrimaryKey();
            if (primaryKey == null) { throw new NotSupportedException(); }

            var statement = new SelectStatement<T>();
            statement = statement.Where(primaryKey, id);

            return Enumerable.Cast<T>(MakeLoader(context, typeof(T), statement));
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
    }

    public class Repository<T> : ObjectRepository where T : class
    {
        public IEnumerable<T> Load(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T>> f)
        {
            return base.Load<T>(context, f);
        }

        public IEnumerable<T2> Load<T2>(IDatabaseContext context, Func<SelectStatement<T>, SelectStatement<T2>> f)
        {
            return base.Load<T, T2>(context, f);
        }

        public IEnumerable<T> Load(IDatabaseContext context, Expression<Func<T, bool>> f)
        {
            return base.Load<T>(context, f);
        }

        public IEnumerable<T> Load(IDatabaseContext context, params object[] id)
        {
            return base.Load<T>(context, id);
        }

        public bool Store(IDatabaseContext context, params T[] objs)
        {
            return base.Store<T>(context, objs);
        }

        public bool Delete(IDatabaseContext context, Expression<Func<T, bool>> f)
        {
            return base.Delete<T>(context, f);
        }

        public bool Delete(IDatabaseContext context, params object[] id)
        {
            return base.Delete<T>(context, id);
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
