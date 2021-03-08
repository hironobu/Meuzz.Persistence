using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class SqlStatement
    {
        public SqlStatement()
        {
        }
    }


    public abstract class SqlSelectStatement : SqlStatement
    {
        public Expression Condition { get; set; }

        public ParamInfo ParamInfo { get; set; } = new ParamInfo();

        public SqlSelectStatement(Type t) : base() { }

        public BindingSpec GetBindingSpecByParamName(string from, string to)
        {
            foreach (var spec in _bindings)
            {
                if (spec.PrimaryParamName == from && spec.ForeignParamName == to)
                {
                    return spec;
                }
            }
            return null;
        }

        public void SetBindingSpecByParamName(BindingSpec spec)
        {
            if (GetBindingSpecByParamName(spec.PrimaryParamName, spec.ForeignParamName) != null)
            {
                throw new NotImplementedException();
            }

            _bindings.Add(spec);
        }

        private List<BindingSpec> _bindings = new List<BindingSpec>();

        public IEnumerable<BindingSpec> GetAllBindings()
        {
            return _bindings;
        }

        /*public IEnumerable<BindingSpec> GetBindingsForPrimaryParamName(string x)
        {
            foreach (var spec in _bindings)
            {
                if (spec.PrimaryParamName == x)
                {
                    yield return spec;
                }
            }
        }*/

        protected SqlSelectStatement BuildCondition(LambdaExpression cond)
        {
            if (!(cond is LambdaExpression lme))
            {
                throw new NotImplementedException();
            }

            var p = lme.Parameters.First<ParameterExpression>();
            //var pel = _sqlBuilder.BuildCondition(null, p) as SqlParameterElement;
            var defaultParamName = ParamInfo.GetDefaultParamName();
            if (defaultParamName == null)
            {
                ParamInfo.RegisterParameter(p.Name, p.Type, true);
            }
            else if (defaultParamName != p.Name)
            {
                throw new NotImplementedException();
            }

            //this.Root = _sqlBuilder.BuildCondition(this.Root, lme.Body);
            this.Condition = cond;
            return this;
        }

        protected void BuildCondition<T>(string key, object[] value)
        {
            var t = typeof(T);
            var px = Expression.Parameter(t, "x");
            Expression f = null;

            if (value.Length == 1)
            {
                f = Expression.Equal(
                    Expression.MakeMemberAccess(px, t.GetPrimaryPropertyInfo()),
                    Expression.Constant(value[0]));
            }
            else
            {
                var tt = value.GetType();
                var ppi = t.GetPrimaryPropertyInfo();
                var ff = typeof(PersistentExpressionExtensions).GetMethod("Contains", BindingFlags.Public | BindingFlags.Static).MakeGenericMethod(typeof(object), ppi.PropertyType);
                f = Expression.Call(ff,
                    Expression.Constant(value),
                    Expression.MakeMemberAccess(px, ppi)
                    );
            }

            BuildCondition(Expression.Lambda(f, px));
        }

        protected SqlSelectStatement BuildBindingCondition(Expression propexp, Expression cond)
        {
            var lambdaexp = (propexp as LambdaExpression).Body;
            var paramexp = (propexp as LambdaExpression).Parameters[0] as ParameterExpression;
            // var primaryName = paramexp.Name;
            var memberInfo = (lambdaexp as MemberExpression).Member;

            var bindingSpec = BindingSpec.Build(paramexp.Type, paramexp.Name, memberInfo, "t", cond);
            bindingSpec.ForeignParamName = ParamInfo.RegisterParameter(bindingSpec.ForeignParamName, bindingSpec.ForeignType, false);
            SetBindingSpecByParamName(bindingSpec);
            return this;
        }

    }

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left = null;
        public T1 Right = null;
    }

    public class StatementProcessor<T> : IEnumerable<T> where T : class, new()
    {
        public SelectStatement<T> Statement { get; set; } = null;
        public Func<SelectStatement<T>, IEnumerable<T>> OnExecute { get; set; } = null;

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return OnExecute(this.Statement).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return OnExecute(this.Statement).GetEnumerator();
        }

    }


    public class SelectStatement<T> : SqlSelectStatement where T : class, new()
    {

        public SelectStatement() : base(typeof(T))
        {
        }

        public virtual SelectStatement<T> Where(Expression<Func<T, bool>> cond)
        {
            BuildCondition(cond);
            return this;
        }

        public virtual SelectStatement<T> Where(string key, params object[] value)
        {
            BuildCondition<T>(key, value);
            return this;
        }

        public virtual SelectStatement<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            BuildBindingCondition(propexp, cond);
            return this;
        }
    }

    public class SqlInsertOrUpdateStatement : SqlStatement
    {
        public string TableName { get; private set; }
        public string PrimaryKey { get; private set; }
        public string[] Columns { get; private set; }

        public object[] Values { get => _values.ToArray(); }

        public IDictionary<string, object> ExtraData { get; set; }

        public bool IsInsert { get; private set; }

        public bool IsBulk { get;  set; }

        private List<object> _values = new List<object>();

        public SqlInsertOrUpdateStatement(Type t, bool isInsert, bool isBulk = false)
        {
            TableName = t.GetTableName();
            var tableInfo = t.GetTableInfo();
            PrimaryKey = t.GetPrimaryKey();
            Columns = tableInfo.Columns.Select(x => x.Name).Where(x => x != t.GetPrimaryKey()).ToArray();
            IsInsert = isInsert;
            IsBulk = isBulk;
        }

        public virtual void Append<T>(IEnumerable<T> objs)
        {
            _values.AddRange(Enumerable.Cast<object>(objs));
        }
    }

    public class SqlDeleteStatement : SqlStatement
    {
        public string TableName { get; set; }
        public Expression Condition { get; set; }

        public ParamInfo ParamInfo { get; set; } = new ParamInfo();


        public SqlDeleteStatement(Type t)
        {
            TableName = t.GetTableName();
        }

        public virtual void BuildCondition(Expression cond)
        {
            if (!(cond is LambdaExpression lme))
            {
                throw new NotImplementedException();
            }

            var p = lme.Parameters.First<ParameterExpression>();
            //var pel = _sqlBuilder.BuildCondition(null, p) as SqlParameterElement;
            var defaultParamName = ParamInfo.GetDefaultParamName();
            if (defaultParamName == null)
            {
                ParamInfo.RegisterParameter(p.Name, p.Type, true);
            }
            else if (defaultParamName != p.Name)
            {
                throw new NotImplementedException();
            }

            this.Condition = cond;
        }

        public virtual void BuildCondition<T>(string key, params object[] value)
        {
            var t = typeof(T);
            var px = Expression.Parameter(t, "x");
            Expression f = null;

            if (value.Length == 1)
            {
                f = Expression.Equal(
                    Expression.MakeMemberAccess(px, t.GetPrimaryPropertyInfo()),
                    Expression.Constant(value[0]));
            }
            else
            {
                var tt = value.GetType();
                var ppi = t.GetPrimaryPropertyInfo();
                var ff = typeof(PersistentExpressionExtensions).GetMethod("Contains", BindingFlags.Public | BindingFlags.Static).MakeGenericMethod(typeof(object), ppi.PropertyType);
                f = Expression.Call(ff,
                    Expression.Constant(value),
                    Expression.MakeMemberAccess(px, ppi)
                    );
            }

            BuildCondition(Expression.Lambda<Func<T, bool>>(f, px));
        }

    }

    public class InsertOrUpdateStatement<T> : SqlInsertOrUpdateStatement where T : class, new()
    {
        public InsertOrUpdateStatement(bool isInsert) : base(typeof(T), isInsert)
        {
        }
    }

    public class InsertStatement<T> : InsertOrUpdateStatement<T> where T : class, new()
    {
        public InsertStatement() : base(true) { }
    }

    public class UpdateStatement<T> : InsertOrUpdateStatement<T> where T : class, new()
    {
        public UpdateStatement() : base(false) { }
    }

    public class DeleteStatement<T> : SqlDeleteStatement where T : class, new()
    {
        public DeleteStatement() : base(typeof(T))
        {

        }

        public virtual DeleteStatement<T> Where(Expression<Func<T, bool>> cond)
        {
            BuildCondition(cond);
            return this;
        }

        public virtual DeleteStatement<T> Where(string key, params object[] value)
        {
            BuildCondition<T>(key, value);
            return this;
        }
    }

    public static class PersistentExpressionExtensions
    {
        public static bool Contains<T, TT>(this T[] objs, TT target)
        {
            return objs.Contains(target);
        }
    }
}
