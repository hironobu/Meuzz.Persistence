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

        public IEnumerable<BindingSpec> GetBindingsForPrimaryParamName(string x)
        {
            foreach (var spec in _bindings)
            {
                if (spec.PrimaryParamName == x)
                {
                    yield return spec;
                }
            }
        }


    }

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left = null;
        public T1 Right = null;
    }


    public class BindingSpec
    {
        public string PrimaryKey { get; set; } = null;
        public string ForeignKey { get; set; } = null;
        public string[] Parameters { get; set; } = null;

        public Type PrimaryType = null;
        public string PrimaryParamName { get; set; } = null;
        public Type ForeignType = null;
        public string ForeignParamName { get; set; }

        // public Func<dynamic, dynamic, bool> Condition = null;
        // public string ConditionSql = null;
        public string ConditionSql { get => GetConditionSql(); }

        public Func<dynamic, dynamic, bool> ConditionFunc
        {
            get 
            {
                if (ConditionParams == null)
                {
                    return _defaultConditionFunc;
                }

                return GetConditionFunc();
            }
        }
        private Func<dynamic, dynamic, bool> _defaultConditionFunc = null;

        public (Func<dynamic, dynamic, bool>, dynamic, dynamic)? ConditionParams { get; set; } = null;

        public MemberInfo MemberInfo { get; set; } = null;

        public BindingSpec()
        {
        }

        public BindingSpec(string fk, string pk)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            _defaultConditionFunc = MakeDefaultFunc(ForeignKey, PrimaryKey);
            // ConditionSql = MakeDefaultJoinConditionSql(ForeignKey, PrimaryKey);
        }


        private string GetConditionSql()
        {
            return $"{PrimaryParamName}.{PrimaryKey ?? PrimaryType.GetPrimaryKey()} {"="} {ForeignParamName}.{ForeignKey}";
        }

        private Func<dynamic, dynamic, bool> GetConditionFunc()
        {
            Func<Func<dynamic, dynamic, bool>, dynamic, dynamic, bool> evaluator = (f, xx, yy) => f(Evaluate(xx), Evaluate(yy));

            return (x, y) => evaluator(ConditionParams.Value.Item1, ConditionParams.Value.Item2(x), ConditionParams.Value.Item3(y));
        }

        private static Func<dynamic, dynamic, bool> MakeDefaultFunc(string foreignKey, string primaryKey)
        {
            Func<Func<dynamic, dynamic, bool>, Func<dynamic, dynamic>, Func<dynamic, dynamic>, Func<dynamic, dynamic, bool>> joiningConditionMaker
                = (Func<dynamic, dynamic, bool> eval, Func<dynamic, dynamic> f, Func<dynamic, dynamic> g) => (dynamic x, dynamic y) => eval(f(x), g(y));
            Func<dynamic, dynamic, bool> eq = (x, y) => x == y;
            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
            Func<string, Func<dynamic, dynamic>> dictionaryGetter = (string key) => (dynamic x) => x[key];
            Func<string, Func<dynamic, dynamic>> memberAccessor = (string memb) => (dynamic x) =>
            {
                Type t = x.GetType();
                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                {
                    return dictionaryGetter(memb)(x);
                }
                else
                {
                    return propertyGetter(memb)(x);
                }
            };

            return joiningConditionMaker(eq, memberAccessor(primaryKey), memberAccessor(foreignKey));
        }

        private static dynamic Evaluate(BindingConditionElement el)
        {
            Func<dynamic, string, dynamic> propertyGetter = (dynamic x, string prop) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
            Func<dynamic, string, dynamic> dictionaryGetter = (dynamic x, string key) => x[StringUtils.ToSnake(key)];

            var arr = el.Evaluate().ToArray();
            var obj = arr.First();
            var propkeys = arr.Skip(1).Select(x => x.Name);
            var prop = string.Join("_", propkeys);

            Type t = obj.GetType();
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                return dictionaryGetter(obj, prop);
            }
            else
            {
                return propertyGetter(obj, prop);
            }
        }

        public class BindingConditionEntry
        {
            public Func<dynamic, dynamic> f { get; set; } = null;
            public ParameterExpression e { get; set; } = null;

            public string[] path { get; set; } = null;
        }

        private static BindingConditionEntry MakeConditionFunc_(Expression exp)
        {
            switch (exp)
            {
                case MemberExpression me:
                    var entry = MakeConditionFunc_(me.Expression);
                    var member = me.Member;
                    return new BindingConditionEntry() { f = (x) => new BindingConditionElement(entry.f(x), member), e = entry.e, path = entry.path.Concat(new string[] { member.Name }).ToArray() };

                case ParameterExpression pe:
                    return new BindingConditionEntry() { f = (x) => x, e = pe, path = new string[] { } };

                default:
                    throw new NotImplementedException();
            }
        }


        public static (Func<dynamic, dynamic, bool>, BindingConditionEntry, BindingConditionEntry) New(Type t, Expression exp, ConditionContext context = null)
        {
            Func<Func<dynamic, dynamic>, Func<dynamic, dynamic>, Func<dynamic, dynamic, bool>, Func<dynamic, dynamic, bool>> conditionFuncMaker = (Func<dynamic, dynamic> f, Func<dynamic, dynamic> g, Func<dynamic, dynamic, bool> ev) => (dynamic x, dynamic y) => ev(f(x), g(y)); // propertyGetter(defaultType, primaryKey)(l) == dictionaryGetter(foreignKey)(r);
            if (exp == null)
            {
                throw new NotImplementedException();
            }

            switch (exp)
            {
                case BinaryExpression bine:
                    var left = MakeConditionFunc_(bine.Left);
                    var right = MakeConditionFunc_(bine.Right);

                    switch (bine.NodeType)
                    {
                        case ExpressionType.Equal:
                            Func<dynamic, dynamic, bool> eq = (x, y) => x == y;
                            return (eq, left, right);

                        case ExpressionType.NotEqual:
                            Func<dynamic, dynamic, bool> ne = (x, y) => x != y;
                            return (ne, left, right);

                        case ExpressionType.LessThan:
                            Func<dynamic, dynamic, bool> lt = (x, y) => x < y;
                            return (lt, left, right);

                        case ExpressionType.LessThanOrEqual:
                            Func<dynamic, dynamic, bool> lte = (x, y) => x <= y;
                            return (lte, left, right);

                        case ExpressionType.GreaterThan:
                            Func<dynamic, dynamic, bool> gt = (x, y) => x > y;
                            return (gt, left, right);

                        case ExpressionType.GreaterThanOrEqual:
                            Func<dynamic, dynamic, bool> gte = (x, y) => x >= y;
                            return (gte, left, right);
                    }
                    break;
            }

            throw new NotImplementedException();
        }

        public static BindingSpec Build<T, T2>(Type primaryType, string primaryName, MemberInfo memberInfo, string defaultForeignParamName, Expression<Func<T, T2, bool>> condexp)
        {
            var propinfo = (memberInfo.MemberType == MemberTypes.Property) ? (memberInfo as PropertyInfo) : null;
            var t = propinfo.PropertyType;
            if (t.IsGenericType)
            {
                t = t.GetGenericArguments()[0];
            }

            BindingSpec bindingSpec = null;
            if (condexp != null)
            {
                if (!(condexp is LambdaExpression lme))
                {
                    throw new NotImplementedException();
                }

                var context = new ConditionContext();
                context.PrimaryType = memberInfo.DeclaringType;
                var parameters = lme.Parameters.Select(x => x.Name).ToArray();

                var bindingParams = BindingSpec.New(memberInfo.DeclaringType, lme.Body, context);

                var primaryKey = StringUtils.ToSnake(string.Join("_", bindingParams.Item2.e.Type == context.PrimaryType ? bindingParams.Item2.path : bindingParams.Item3.path));
                var foreignKey = StringUtils.ToSnake(string.Join("_", bindingParams.Item2.e.Type != context.PrimaryType ? bindingParams.Item2.path : bindingParams.Item3.path));

                var fs = new Func<dynamic, dynamic>[parameters.Length];
                fs[Array.IndexOf(parameters, bindingParams.Item2.e.Name)] = bindingParams.Item2.f;
                fs[Array.IndexOf(parameters, bindingParams.Item3.e.Name)] = bindingParams.Item3.f;

                bindingSpec = new BindingSpec()
                {
                    PrimaryKey = primaryKey,
                    ForeignKey = foreignKey,
                    // ConditionSql = FormatElement(lme),
                    ConditionParams = (bindingParams.Item1, fs[0], fs[1])
                };
            }

            if (bindingSpec == null)
            {
                var hasmany = memberInfo.GetCustomAttribute<HasManyAttribute>();
                if (hasmany != null)
                {
                    bindingSpec = new BindingSpec(hasmany.ForeignKey, hasmany.PrimaryKey ?? "id");
                }
                else
                {
                    var primaryTable = memberInfo.DeclaringType.GetTableName();
                    var foreignTableInfo = t.GetTableInfo();
                    var matched = foreignTableInfo.Columns.Where(x => x.BindingTo == primaryTable).First();
                    bindingSpec = new BindingSpec(matched.Name.ToLower(), matched.BindingToPrimaryKey.ToLower());
                }
            }

            bindingSpec.PrimaryType = primaryType;
            bindingSpec.PrimaryParamName = primaryName;
            bindingSpec.ForeignType = t;
            bindingSpec.ForeignParamName = defaultForeignParamName;
            bindingSpec.MemberInfo = memberInfo;

            return bindingSpec;
        }


        public class ConditionContext
        {
            public Type PrimaryType { get; set; } = null;
            // public string[] Parameters { get; set; } = null;
            // public MemberInfo MemberInfo { get; set; } = null;
        }

        public class BindingConditionElement
        {
            public dynamic Left;
            public dynamic Right;

            public BindingConditionElement(dynamic l, dynamic r)
            {
                this.Left = l;
                this.Right = r;
            }

            public dynamic[] Evaluate()
            {
                var ret = new List<dynamic>();

                if (Left is BindingConditionElement)
                {
                    ret.AddRange(Left.Evaluate());
                }
                else
                {
                    ret.Add(Left);
                }
                if (Right is BindingConditionElement)
                {
                    ret.AddRange(Right.Evaluate());
                }
                else
                {
                    ret.Add(Right);
                }

                return ret.ToArray();
            }
        }
    }

    /*
    public interface IFilterable<T> : IEnumerable<T> where T : class, new()
    {
        IFilterable<T> And(Expression<Func<T, bool>> cond);

        IFilterable<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new();
    }*/

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

        public virtual SelectStatement<T> Where(string key, params object[] value)
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

            return Where(Expression.Lambda<Func<T, bool>>(f, px));
        }

        public virtual SelectStatement<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            var lambdaexp = (propexp as LambdaExpression).Body;
            var paramexp = (propexp as LambdaExpression).Parameters[0] as ParameterExpression;
            // var primaryName = paramexp.Name;
            var memberInfo = (lambdaexp as MemberExpression).Member;

            var bindingSpec = BindingSpec.Build(paramexp.Type, paramexp.Name, memberInfo, "t", cond);
            RegisterBindingSpec(bindingSpec);
            return this;
        }

        private void RegisterBindingSpec(BindingSpec bindingSpec)
        {
            bindingSpec.ForeignParamName = ParamInfo.RegisterParameter(bindingSpec.ForeignParamName, bindingSpec.ForeignType, false);
            SetBindingSpecByParamName(bindingSpec);
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

        public virtual DeleteStatement<T> And(Expression<Func<T, bool>> cond)
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
            return this;
        }

        public virtual DeleteStatement<T> And(string key, params object[] value)
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

            return And(Expression.Lambda<Func<T, bool>>(f, px));
        }
    }

    public static class PersistentExpressionExtensions
    {
        public static bool Contains<T, TT>(this T[] objs, TT target)
        {
            return objs.Contains(target);
        }
    }

/*
    public class SqlElement
    {
    }

    public enum SqlElementVerbs
    {
        None,
        And,
        Or,
        Lt,
        Lte,
        Gt,
        Gte,
        Eq,
        Ne,
        MemberAccess,
        // Joins,
        Lambda,
        Select,
        Join,
    }*/

    /*
    public class SqlJoinElement : SqlBinaryElement
    {
        public MemberInfo MemberInfo = null;
        public BindingSpec BindingSpec = null;

        public SqlJoinElement(SqlElement left, SqlElement right, MemberInfo memberInfo, BindingSpec bindingSpec) : base(SqlElementVerbs.Join, left, right)
        {
            MemberInfo = memberInfo;
            BindingSpec = bindingSpec;
        }
    }*/
/*
    public class SqlParameterElement : SqlElement
    {
        public Type Type;
        public string Name { get; set; }

        public SqlParameterElement(Type type, string key)
        {
            Type = type;
            Name = key.ToLower();
        }
    }

    public class SqlBinaryElement : SqlElement
    {
        public SqlElementVerbs Verb = default(SqlElementVerbs);

        public SqlElement Left = null;
        public SqlElement Right = null;

        public SqlBinaryElement(SqlElementVerbs v, SqlElement x, SqlElement y)
        {
            Verb = v;
            Left = x;
            Right = y;
        }

        public override string ToString()
        {
            return $"[{Verb} {Left} {Right}]";
        }
    }

    public class SqlLeafElement : SqlElement
    {
        public object Value = default(object);

        public SqlLeafElement(object value)
        {
            Value = value;
        }

        public override string ToString()
        {
            return Value.ToString();
        }
    }

    public class SqlConstantElement : SqlLeafElement
    {
        public SqlConstantElement(object value) : base(value) { }
    }
*/
}
