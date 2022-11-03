#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;

namespace Meuzz.Persistence.Sql
{
    public class SqlSelectStatement : SqlConditionalStatement
    {
        public SqlSelectStatement(Type t) : base(t)
        {
            ParameterSetInfo = new ParameterSetInfo();
            ParameterSetInfo.RegisterParameter(null, t, true);
        }

        public SqlSelectStatement(SqlSelectStatement statement) : base(statement.Type, statement.Condition)
        {
            if (statement.Source != null)
            {
                throw new NotImplementedException();
            }
            if (statement.ColumnSpecs != null && statement.ColumnSpecs.Any())
            {
                throw new NotImplementedException();
            }
            #if false
            if (statement.RelationSpecs != null && statement.RelationSpecs.Any())
            {
                throw new NotImplementedException();
            }
            #endif
            if (statement.OutputSpec != null)
            {
                throw new NotImplementedException();
            }

            _source = statement.Source;
            _columnSpecs = statement.ColumnSpecs;
            _relationSpecs = statement.RelationSpecs.ToList();
            _outputSpec = statement.OutputSpec;

            ParameterSetInfo = new ParameterSetInfo(statement.ParameterSetInfo);
        }

        public SqlSelectStatement? Source { get => _source; }

        public ColumnSpec[] ColumnSpecs { get => _columnSpecs; }

        public IEnumerable<RelationSpec> RelationSpecs => _relationSpecs;

        public OutputSpec? OutputSpec { get => _outputSpec; }

        public Type OutputType { get => _outputSpec?.OutputExpression.ReturnType ?? Type; }

        [Obsolete]
        public ParameterSetInfo ParameterSetInfo { get; }

        public Func<object, object>? PackerFunc => _packerFunc;

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

        #region Condition
        public override void BuildCondition(LambdaExpression cond, Type? t)
        {
            var px0 = cond.Parameters.First();
            var px = Expression.Parameter(px0.Type, ParameterSetInfo.GetDefaultParamName());
            cond = Expression.Lambda(ParameterReplacer.Replace(cond.Body, cond.Parameters.First(), px), px);

            var p = cond.Parameters.Single();
            ParameterSetInfo.RegisterParameter(p.Name, t ?? p.Type, true);
            // (return) == p.Name or with number added

            base.BuildCondition(cond, t);
        }
        #endregion

        #region Columns
        public void BuildColumnSpec(LambdaExpression columnListExpression)
        {
            Expression bodyexp = columnListExpression.Body;
            IEnumerable<Expression> args;
            IEnumerable<MemberInfo> memberInfos;

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
        private void AddRelationSpec(RelationSpec spec)
        {
            if (_relationSpecs.SingleOrDefault(x => x.Left.Name == spec.Left.Name && x.Right.Name == spec.Right.Name) != null)
            {
                throw new NotImplementedException();
            }

            _relationSpecs.Add(spec);
        }

        public void BuildRelationSpec(LambdaExpression propexp, LambdaExpression? condexp)
        {
            var relationSpec = RelationSpec.BuildByPropertyAndCondition(ParameterSetInfo, propexp, condexp);
            AddRelationSpec(relationSpec);
        }

        public void BuildRelationSpec(LambdaExpression? condexp, Type rightParamType, string foreignKey)
        {
            var relationSpec = RelationSpec.BuildByPropertyAndForeignKey(ParameterSetInfo, condexp, Type, rightParamType, foreignKey);
            AddRelationSpec(relationSpec);
        }

        #if false
        public void BuildRightJoinRelationSpec(Type rightParamType, string foreignKey)
        {
            var foreignKeyProperty = Type.GetPropertyInfoFromColumnName(foreignKey);
            var primaryKeyProperty = rightParamType.GetPropertyInfoFromColumnName(rightParamType.GetPrimaryKey()!);

            var px = Expression.Parameter(Type, "x");
            var py = Expression.Parameter(rightParamType, "y");

            var lambda = Expression.Lambda(
                Expression.MakeBinary(ExpressionType.Equal,
                Expression.MakeMemberAccess(px, foreignKeyProperty),
                Expression.MakeMemberAccess(py, primaryKeyProperty)),
                px, py);
            BuildRelationSpec(lambda);
        }
        #endif

        public void BuildRelationSpec(LambdaExpression condexp)
        {
            var leftParamType = condexp.Parameters.First().Type;
            var rightParamType = condexp.Parameters.Last().Type;

            var leftParamName = ParameterSetInfo.GetDefaultParamName();
            var rightParamName = ParameterSetInfo.RegisterParameter(condexp.Parameters.Skip(1).First().Name, rightParamType, false);

            var relationSpec = RelationSpec.Build(leftParamName, leftParamType, rightParamName, rightParamType, null, null, condexp);
            AddRelationSpec(relationSpec);

            var oldf = _packerFunc;
            _packerFunc = o =>
            {
                var d = (IDictionary<string, object?>)o;

                var left = d[leftParamName];
                var right = d[rightParamName];

                if (left == null)
                {
                    throw new NotImplementedException();
                }

                return (oldf(left), right);
            };
        }

        #endregion

        #region Output
        public void BuildOutputSpec(LambdaExpression outputexp)
        {
            _outputSpec = new OutputSpec(outputexp);
        }
        #endregion

        private SqlSelectStatement? _source;
        private ColumnSpec[] _columnSpecs = new ColumnSpec[] { };
        private IList<RelationSpec> _relationSpecs = new List<RelationSpec>();
        private OutputSpec? _outputSpec = null;
        private Func<object, object> _packerFunc = o => o;
    }

    public class ColumnSpec
    {
        public string Parameter { get; set; }

        public string Name { get; set; }

        public string? Alias { get; set; }

        public ColumnSpec(string parameter, string name, string? alias = null)
        {
            Parameter = parameter;
            Name = name.ToSnake();
            Alias = alias != null ? alias.ToSnake() : null;
        }
    }

    public class OutputSpec
    {
        public OutputSpec(LambdaExpression expr)
        {
            OutputExpression = GetOutputExpression(expr);

            SourceMemberExpressions = GetSourceMemberExpressions(expr);
        }

        public LambdaExpression OutputExpression { get; }

        public Func<IDictionary<string, object?>, object?> CompiledOutputFunc { get
            {
                if (_compiledOutputFunc == null)
                {
                    _compiledOutputFunc = (Func<IDictionary<string, object?>, object?>)OutputExpression.Compile();
                }
                return _compiledOutputFunc;
            }
        }

        public IDictionary<ExpressionComparer, MemberExpression[]> SourceMemberExpressions { get; }

        private Expression _GetOutputExpression(Expression expr, ParameterExpression[] parameters)
        {
            var mi = new Func<object, object>(x => Convert.ToUInt32(x)).GetMethodInfo();

            switch (expr)
            {
                case NewExpression ne:
                    var args = ne.Arguments.Zip(ne.Members, (a, m) =>
                    {
                        var a1 = _GetOutputExpression(a, parameters);
                        switch (m)
                        {
                            case PropertyInfo pi:
                                switch (pi.PropertyType)
                                {
                                    case Type intType when intType == typeof(int):
                                        return Expression.Convert(Expression.Convert(a1, typeof(long)), typeof(int));
                                    case Type stringType when stringType == typeof(string):
                                        return Expression.Convert(a1, stringType);
                                    default:
                                        return Expression.Convert(a1, pi.PropertyType);
                                }
                            default:
                                throw new NotImplementedException();
                        }
                    });
                    return Expression.New(ne.Constructor, args, ne.Members);

                case MemberExpression me:
                    var memberInfo = me.Member;
                    var px0 = (ParameterExpression)me.Expression;
                    var px = parameters.First(x => x.Name == px0.Name);
                    return ExpressionHelpers.MakeDictionaryAccessorExpression(px, memberInfo.GetColumnName());

                case ParameterExpression pe:
                    return pe;

                default:
                    throw new NotImplementedException();
            }
        }

        private LambdaExpression GetOutputExpression(LambdaExpression outputexp)
        {
            var t1 = typeof(IDictionary<string, object?>);
            var parameters = outputexp.Parameters.Select(x => Expression.Parameter(t1, x.Name)).ToArray();

            var bodyexp = _GetOutputExpression(outputexp.Body, parameters);

            // return Expression.Lambda<Func<IDictionary<string, object?>, object?>>(bodyexp, parameters);
            return Expression.Lambda(bodyexp, parameters);
        }

        private IDictionary<ExpressionComparer, MemberExpression[]> GetSourceMemberExpressions(LambdaExpression outputexp)
        {
            var paramexps = outputexp.Parameters.ToArray();
            var bodyexp = outputexp.Body;
            var exprdict = new Dictionary<ExpressionComparer, List<MemberExpression>>();
            var args = Enumerable.Empty<Expression>(); ;

            if (!outputexp.Parameters.First().Type.IsGenericTuple())
            {
                switch (bodyexp)
                {
                    case NewExpression newe:
                        args = newe.Arguments;
                        break;

                    case MemberExpression me:
                        args = new Expression[] { me };
                        break;
                }

                foreach (var a in args)
                {
                    if ((a is MemberExpression me))
                    {
                        var k = new ExpressionComparer(me.Expression);
                        if (!exprdict.TryGetValue(k, out var vals))
                        {
                            vals = new List<MemberExpression>();
                            exprdict.Add(k, vals);
                        }

                        vals.Add(me);
                    }
                }
            }

            return exprdict.ToDictionary(x => x.Key, x => x.Value.ToArray());
        }

        private Func<IDictionary<string, object?>, object?>? _compiledOutputFunc;
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

        public SelectStatement(SqlSelectStatement statement) : base(statement)
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

        public SelectStatement<T> Join<T1>(Expression<Func<T, IEnumerable<T1>>> propexp, Expression<Func<T, T1, bool>>? cond = null)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildRelationSpec(propexp, cond);
            return statement2;
        }

        public SelectStatement<T> JoinBy<T1>(string foreignKey)
        {
            var statement2 = new SelectStatement<T>(this);
            statement2.BuildRelationSpec(null, typeof(T1), foreignKey);
            return statement2;
        }

        public SelectStatement<(T, T1)> Joins<T1>(Expression<Func<T, T1, bool>> cond)
        {
            var statement2 = new SelectStatement<(T, T1)>(this);
            statement2.BuildRelationSpec(cond);
            return statement2;
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, T1>> expression)
        {
            var statement2 = new SelectStatement<T1>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return statement2;
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, int, T1>> expression)
        {
            var statement2 = new SelectStatement<T1>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return statement2;
        }

        public virtual SelectStatement<T1> Select<T1>(Expression<Func<T, int, IEnumerable<T>, T1>> expression)
        {
            var statement2 = new SelectStatement<T1>(this);
            statement2.BuildColumnSpec(expression);
            statement2.BuildOutputSpec(expression);
            return statement2;
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
