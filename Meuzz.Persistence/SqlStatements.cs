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
        public string TableName { get; private set; }

        public SqlStatement(Type t)
        {
            TableName = t.GetTableName();
        }
    }

    public abstract class SqlConditionalStatement : SqlStatement
    {
        public Expression Condition { get; private set; }

        public ParamInfo ParamInfo { get; } = new ParamInfo();

        public SqlConditionalStatement(Type t) : base(t)
        {
            ParamInfo.RegisterParameter(null, t, true);
        }

        public virtual void BuildCondition(LambdaExpression cond)
        {
            if (!(cond is LambdaExpression lme))
            {
                throw new NotImplementedException();
            }

            var p = cond.Parameters.First<ParameterExpression>();
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
        }

        public virtual void BuildCondition(string key, params object[] value)
        {
            Type t = ParamInfo.GetDefaultParamType();
            var px = Expression.Parameter(t, "x");
            Expression f = null;

            var key0 = key.Replace("_id", "");
            var ppi = string.IsNullOrEmpty(key0)
                ? t.GetPrimaryPropertyInfo()
                : t.GetProperty(StringUtils.ToCamel(key0, true));
            var memberAccessor = Expression.MakeMemberAccess(px, ppi);

            if (key.EndsWith("_id"))
            {
                memberAccessor = Expression.MakeMemberAccess(memberAccessor, ppi.PropertyType.GetPrimaryPropertyInfo());
            }

            if (value.Length == 1)
            {
                f = Expression.Equal(
                    memberAccessor,
                    Expression.Constant(value[0]));
            }
            else
            {
                var ff = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static).Where(x => x.Name == "Contains" && x.GetParameters().Count() == 2).Single()
                    .MakeGenericMethod(typeof(object));
                f = Expression.Call(ff,
                    Expression.Constant(value),
                    Expression.Convert(memberAccessor, typeof(object))
                    );
            }

            BuildCondition(Expression.Lambda(f, px));
        }
    }

    public class SqlSelectStatement : SqlConditionalStatement
    {
        public SqlSelectStatement(Type t) : base(t) { }

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

        protected virtual void BuildBindingCondition(LambdaExpression propexp, LambdaExpression cond)
        {
            var bodyexp = propexp.Body;
            var paramexp = propexp.Parameters[0];
            var memberInfo = (bodyexp as MemberExpression).Member;

            var bindingSpec = BindingSpec.Build(paramexp.Type, paramexp.Name, memberInfo, paramexp.Name, cond);
            bindingSpec.ForeignParamName = ParamInfo.RegisterParameter(bindingSpec.ForeignParamName, bindingSpec.ForeignType, false);
            SetBindingSpecByParamName(bindingSpec);
        }
    }

    public class SqlInsertOrUpdateStatement : SqlStatement
    {
        public string PrimaryKey { get; private set; }
        public string[] Columns { get; private set; }

        public object[] Values { get => _values.ToArray(); }

        public IDictionary<string, object> ExtraData { get; set; }

        public bool IsInsert { get; private set; }

        private List<object> _values = new List<object>();

        public SqlInsertOrUpdateStatement(Type t, bool isInsert) : base(t)
        {
            var tableInfo = t.GetTableInfo();
            PrimaryKey = t.GetPrimaryKey();
            Columns = tableInfo.Columns.Select(x => x.Name).Where(x => x != t.GetPrimaryKey()).ToArray();
            IsInsert = isInsert;
        }

        public virtual void Append<T>(IEnumerable<T> objs)
        {
            _values.AddRange(Enumerable.Cast<object>(objs));
        }
    }

    public class SqlDeleteStatement : SqlConditionalStatement
    {
        public SqlDeleteStatement(Type t) : base(t)
        {
        }
    }

    public class InsertOrUpdateStatement<T> : SqlInsertOrUpdateStatement where T : class, new()
    {
        public InsertOrUpdateStatement(bool isInsert) : base(typeof(T), isInsert)
        {
        }
    }

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left = null;
        public T1 Right = null;
    }

/*    public class StatementProcessor<T> : IEnumerable<T> where T : class, new()
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
    }*/

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
            BuildCondition(key, value);
            return this;
        }

        public virtual SelectStatement<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            BuildBindingCondition(propexp, cond);
            return this;
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
            BuildCondition(key, value);
            return this;
        }
    }
}
