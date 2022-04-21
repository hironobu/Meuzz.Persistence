﻿#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Sql
{
    public class SqlSelectStatement : SqlConditionalStatement
    {
        public SqlSelectStatement(Type t) : base(t)
        {
        }

        public SqlSelectStatement(SqlSelectStatement statement) : base(statement)
        {
            _source = statement.Source;
            _columnSpecs = statement.ColumnSpecs;
            _relationSpecs = statement.RelationSpecs;
            _outputSpec = statement.OutputSpec;
        }

        public SqlSelectStatement? Source { get => _source; }

        public ColumnSpec[] ColumnSpecs { get => _columnSpecs; }

        public RelationSpec[] RelationSpecs { get => _relationSpecs; }
        
        public OutputSpec? OutputSpec { get => _outputSpec; }

        #region Source
        protected void BuildSource(SqlSelectStatement statement)
        {
            _source = statement;

            /*_columnSpecs = statement.ColumnSpecs;
            var paname = ParameterSetInfo.RegisterParameter(null, Type, true);
            foreach (var colspec in _columnSpecs)
            {
                colspec.Parameter = paname;
            }*/
        }
        #endregion

        #region Columns
        public void BuildColumnSpec(LambdaExpression columnlistexp)
        {
            Expression[] paramexps = columnlistexp.Parameters.ToArray();
            Expression bodyexp = columnlistexp.Body;
            IEnumerable<Expression> args;
            IEnumerable<MemberInfo> memberInfos = new MemberInfo[] { };

            switch (bodyexp)
            {
                case NewExpression newe:
                    args = newe.Arguments;
                    memberInfos = newe.Members;
                    break;

                case MemberExpression me:
                    args = new[] { me };
                    memberInfos = new[] { me.Member };
                    break;

                default:
                    throw new InvalidOperationException();
            }

            var columnSpecs = new List<ColumnSpec>();

            foreach (var (a, m) in args.Zip(memberInfos))
            {
                switch (a)
                {
                    case MemberExpression me:
                        var parameterName = ParameterSetInfo.GetName(me.Expression);

                        var spec = new ColumnSpec(parameterName, me.Member.Name, m.Name);
                        columnSpecs.Add(spec);

                        //return exprdict.ToDictionary(x => x.Key, x => x.Value.ToArray());

                        break;

                    default:
                        throw new NotImplementedException($"unavailable expression: {a}");
                }
            }

            _columnSpecs = columnSpecs.ToArray();
        }
        #endregion

        #region Relations
        private RelationSpec GetRelationSpecByParamName(string left, string right)
        {
            return _relationSpecs.SingleOrDefault(spec => spec.Left.Name == left && spec.Right.Name == right);
        }

        private void AddRelationSpec(RelationSpec spec)
        {
            if (GetRelationSpecByParamName(spec.Left.Name, spec.Right.Name) != null)
            {
                throw new NotImplementedException();
            }

            var specs = new RelationSpec[_relationSpecs.Length + 1];
            _relationSpecs.CopyTo(specs, 0);
            specs[_relationSpecs.Length] = spec;

            _relationSpecs = specs;
        }

        protected virtual void BuildRelationSpec(LambdaExpression propexp, LambdaExpression? condexp)
        {
            var propbodyexp = propexp.Body;
            var leftparamexp = propexp.Parameters.Single();
            var memberInfo = ((MemberExpression)propbodyexp).Member;

            var rightParamType = ((PropertyInfo)memberInfo).PropertyType;
            if (rightParamType.IsGenericType)
            {
                rightParamType = rightParamType.GetGenericArguments().First();
            }

            var leftParamName = ParameterSetInfo.GetDefaultParamName();
            if (leftParamName == null)
            {
                leftParamName = ParameterSetInfo.RegisterParameter(leftparamexp.Name, leftparamexp.Type, true);
            }

            string rightParamName = ParameterSetInfo.RegisterParameter(condexp != null ? condexp.Parameters.Skip(1).First().Name : null, rightParamType, false);

            var relationSpec = RelationSpec.Build(leftParamName, leftparamexp.Type, rightParamName, memberInfo, condexp);
            AddRelationSpec(relationSpec);
        }
        #endregion

        #region Output
        protected virtual void BuildOutputSpec(LambdaExpression outputexp)
        {
            _outputSpec = new OutputSpec(outputexp);

            ParameterSetInfo.SetParameterMemberExpressions(_outputSpec.SourceMemberExpressions);
        }
        #endregion

        private SqlSelectStatement? _source;
        private ColumnSpec[] _columnSpecs = new ColumnSpec[] { };
        private RelationSpec[] _relationSpecs = new RelationSpec[] { };
        private OutputSpec? _outputSpec = null;
    }

    public class ColumnSpec
    {
        public string Parameter { get; set; }

        public string Name { get; set; }

        public string? Alias { get; set; }

        public ColumnSpec(string parameter, string name, string? alias = null)
        {
            Parameter = parameter;
            Name = StringUtils.ToSnake(name);
            Alias = alias != null ? StringUtils.ToSnake(alias) : null;
        }
    }

    public class OutputSpec
    {
        public OutputSpec(LambdaExpression expr)
        {
            OutputExpression = expr;

            SourceMemberExpressions = GetSourceMemberExpressions(expr);
        }

        public LambdaExpression OutputExpression { get; }

        public IDictionary<ExpressionComparer, MemberExpression[]> SourceMemberExpressions { get; }

        private IDictionary<ExpressionComparer, MemberExpression[]> GetSourceMemberExpressions(LambdaExpression outputexp)
        {
            Expression[] paramexps = outputexp.Parameters.ToArray();
            Expression bodyexp = outputexp.Body;
            var exprdict = new Dictionary<ExpressionComparer, List<MemberExpression>>();
            IEnumerable<Expression> args;

            switch (bodyexp)
            {
                case NewExpression newe:
                    args = newe.Arguments;
                    break;

                case MemberExpression me:
                    args = new Expression[] { me };
                    break;

                default:
                    throw new InvalidOperationException();
            }

            foreach (var a in args)
            {
                if (!(a is MemberExpression me))
                {
                    throw new NotImplementedException($"unavailable expression: {a}");
                }

                var k = new ExpressionComparer(me.Expression);
                if (!exprdict.TryGetValue(k, out var vals))
                {
                    vals = new List<MemberExpression>();
                    exprdict.Add(k, vals);
                }

                vals.Add(me);
            }

            return exprdict.ToDictionary(x => x.Key, x => x.Value.ToArray());
        }
    }

    public class ExpressionComparer
    {
        public ExpressionComparer(Expression expr)
        {
            Expression = expr;
        }

        public Expression Expression { get; }

        public override bool Equals(object? obj)
        {
            if (Object.ReferenceEquals(this, obj))
            {
                return true;
            }

            if (!(obj is ExpressionComparer comparer))
            {
                return false;
            }

            return ExpressionEquals(Expression, comparer.Expression);
        }

        private static bool ExpressionEquals(Expression e1, Expression e2)
        {
            if (e1.NodeType != e2.NodeType)
            {
                return false;
            }

            switch (e1)
            {
                case MemberExpression me1:
                    var me2 = (MemberExpression)e2;
                    return ExpressionEquals(me1.Expression, me2.Expression) && me1.Member == me2.Member && me1.Type == me2.Type;

                case ParameterExpression pe1:
                    var pe2 = (ParameterExpression)e2;
                    return pe1.Name == pe2.Name && pe1.Type == pe2.Type;

                default:
                    throw new NotImplementedException();
            }
        }


        public override int GetHashCode()
        {
            return GetHashCode(Expression);
        }

        private int GetHashCode(MemberInfo memberInfo)
        {
            return memberInfo.GetHashCode();
        }

        private int GetHashCode(Expression expr)
        {
            switch (Expression)
            {
                case MemberExpression me:
                    int h = me.Member.GetHashCode();
                    Console.WriteLine(h);
                    return GetHashCode(me.Member) + GetHashCode(me.Expression);

                case ParameterExpression pe:
                    return pe.Name.GetHashCode() + pe.Type.GetHashCode();

                default:
                    throw new NotImplementedException();
            }
        }

        public static bool operator ==(ExpressionComparer? c1, ExpressionComparer? c2)
        {
            return c1?.Equals(c2) == true;
        }

        public static bool operator !=(ExpressionComparer? c1, ExpressionComparer? c2)
        {
            return !(c1 == c2);
        }

        public static implicit operator ExpressionComparer(Expression expr)
        {
            return new ExpressionComparer(expr);
        }

        public static implicit operator Expression (ExpressionComparer comparer)
        {
            return comparer.Expression;
        }
    }

    public class OutputSpec<T, T1>
    {
        public Expression<Func<T, int, IEnumerable<T>, T1>> OutputExpression { get; }

        public OutputSpec(Expression<Func<T, int, IEnumerable<T>, T1>> expr)
        {
            OutputExpression = expr;
        }
    }

    public class SelectStatement<T> : SqlSelectStatement
    {
        public SelectStatement() : base(typeof(T))
        {
        }

        public SelectStatement(SelectStatement<T> statement) : base(statement)
        {
        }

        public static SelectStatement<T1> Create<T1>(SelectStatement<T>? source)
        {
            var statement = new SelectStatement<T1>();
            if (source != null)
            {
                statement = statement.From(source);
            }
            return statement;
        }

        public virtual SelectStatement<T> From<T0>(SelectStatement<T0> statement)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildSource(statement);
            return statement2;
        }

        public virtual SelectStatement<T> Where(Expression<Func<T, bool>> cond)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildCondition(cond, null);
            return statement2;
        }

        public virtual SelectStatement<T> Where(string key, params object[] value)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildCondition(key, value);
            return statement2;
        }

        public virtual SelectStatement<T> Joins<T1>(Expression<Func<T, IEnumerable<T1>>> propexp, Expression<Func<T, T1, bool>>? cond = null)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildRelationSpec(propexp, cond);
            return statement2;
        }

        public virtual SelectStatement<(T _0, T1 _1)> Joins<T1>(Expression<Func<T, T1, bool>>? cond)
        {
            throw new NotImplementedException();
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, T1>> expression)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return Create<T1>(statement2);
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, int, T1>> expression)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return Create<T1>(statement2);
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, int, IEnumerable<T>, T1>> expression)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return Create<T1>(statement2);
        }

        public static implicit operator T(SelectStatement<T> self)
        {
            throw new NotImplementedException();
        }
    }

    public class SqlDeleteStatement : SqlConditionalStatement
    {
        public SqlDeleteStatement(Type t) : base(t) { }
    }

    public class DeleteStatement<T> : SqlDeleteStatement
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
