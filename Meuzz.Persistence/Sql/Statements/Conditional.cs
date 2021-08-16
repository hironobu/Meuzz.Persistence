using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence.Sql
{
    public class SqlSelectStatement : SqlConditionalStatement
    {
        public SqlSelectStatement(Type t) : base(t) { }

        public SqlSelectStatement(SqlSelectStatement statement) : base(statement)
        {
            _columnInfos = statement.ColumnInfos.ToList();
            _relationSpecs = statement.RelationSpecs.ToList();
        }

        public IEnumerable<MemberInfo> ColumnInfos { get => _columnInfos; }
        public IEnumerable<RelationSpec> RelationSpecs { get => _relationSpecs; }

        protected virtual void AddColumnInfos(MemberInfo memberInfo)
        {
            _columnInfos.Add(memberInfo);
        }

        private RelationSpec GetRelationSpecByParamName(string from, string to)
        {
            return _relationSpecs.SingleOrDefault(spec => spec.Primary.Name == from && spec.Foreign.Name == to);
        }

        private void SetRelationSpecByParamName(RelationSpec spec)
        {
            if (GetRelationSpecByParamName(spec.Primary.Name, spec.Foreign.Name) != null)
            {
                throw new NotImplementedException();
            }

            _relationSpecs.Add(spec);
        }

        protected virtual void BuildRelationSpec(LambdaExpression propexp, LambdaExpression cond)
        {
            var bodyexp = propexp.Body;
            var paramexp = propexp.Parameters[0];
            var memberInfo = (bodyexp as MemberExpression).Member;

            var bindingSpec = RelationSpec.Build(paramexp.Type, paramexp.Name, memberInfo, paramexp.Name, cond);
            bindingSpec.Foreign.Name = ParamInfo.RegisterParameter(bindingSpec.Foreign.Name, bindingSpec.Foreign.Type, false);
            SetRelationSpecByParamName(bindingSpec);
        }

        private List<MemberInfo> _columnInfos = new List<MemberInfo>();
        private List<RelationSpec> _relationSpecs = new List<RelationSpec>();
    }

    public class SqlDeleteStatement : SqlConditionalStatement
    {
        public SqlDeleteStatement(Type t) : base(t)
        {
        }
    }

    public class SelectStatement<T> : SqlSelectStatement where T : class, new()
    {
        public SelectStatement() : base(typeof(T))
        {
        }

        public SelectStatement(SelectStatement<T> statement) : base(statement)
        {
        }

        public virtual SelectStatement<T> Where(Expression<Func<T, bool>> cond)
        {
            BuildCondition(cond, null);
            return this;
        }

        public virtual SelectStatement<T> Where(string key, params object[] value)
        {
            BuildCondition(key, value);
            return this;
        }

        public virtual SelectStatement<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            BuildRelationSpec(propexp, cond);
            return this;
        }

        public virtual SelectStatement<T> Select<T2>(Expression<Func<T, T2>> expression) where T2 : class
        {
            var members = PickupMemberExpressions(expression);
            var t = members.Item1;
            var ctors = t.GetConstructors();
            var paraminfos = ctors.First().GetParameters();
            var obj = Activator.CreateInstance(t);
            return null;
        }


        private (Type, IEnumerable<MemberInfo>) PickupMemberExpressions(Expression e)
        {
            switch (e)
            {
                case LambdaExpression le:
                    return PickupMemberExpressions(le.Body);

                case BinaryExpression be:
                    var left = PickupMemberExpressions(be.Left);
                    var right = PickupMemberExpressions(be.Right);
                    return (null, left.Item2.Concat(right.Item2));

                case MemberExpression me:
                    return (null, new[] { me.Member });

                case NewExpression ne:
                    return (ne.Type, ne.Arguments.Select(PickupMemberExpressions).SelectMany(x => x.Item2));

                default:
                    throw new NotImplementedException();
            }
        }
    }

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left { get; set; }

        public T1 Right { get; set; }
    }

    public class DeleteStatement<T> : SqlDeleteStatement where T : class, new()
    {
        public DeleteStatement() : base(typeof(T))
        {
        }

        public virtual DeleteStatement<T> Where(Expression<Func<T, bool>> cond)
        {
            BuildCondition(cond, null);
            return this;
        }

        public virtual DeleteStatement<T> Where(string key, params object[] value)
        {
            BuildCondition(key, value);
            return this;
        }
    }
}
