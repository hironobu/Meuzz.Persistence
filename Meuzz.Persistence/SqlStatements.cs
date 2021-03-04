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
        public SqlElement Root { get; set; }

        public ParamInfo ParamInfo { get; set; } = new ParamInfo();

        public SqlStatement()
        {
        }
    }


    public abstract class SqlSelectStatement : SqlStatement
    {
        public SqlSelectStatement() : base() { }

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


        protected virtual SqlElement BuildElement(SqlElement parent, Expression expression)
        {
            switch (expression)
            {
                case LambdaExpression mce:
                    var p = mce.Parameters.First<ParameterExpression>();
                    var pel = BuildElement(parent, p) as SqlParameterElement;
                    pel.Name = ParamInfo.RegisterParameter(pel.Name, pel.Type, true);
                    return MakeSqlElement(mce.NodeType, BuildElement(parent, mce.Body), pel);

                case BinaryExpression be:
                    var lstr = BuildElement(parent, be.Left);
                    var rstr = BuildElement(parent, be.Right);
                    // return $"[{be.NodeType} <{lstr}> <{rstr}>]";
                    return MakeSqlElement(be.NodeType, lstr, rstr);

                case ParameterExpression pe:
                    // var mstr = GetColumnNameFromProperty(pe.);
                    return MakeSqlElement(pe);

                //case Property pie:
                //    return $"<P <{pie.ToString()}>>"
                case MemberExpression me:
                    return MakeSqlElement(me.NodeType, BuildElement(parent, me.Expression), MakeSqlElement(me));

                case ConstantExpression ce:
                    return MakeSqlElement(ce);

                default:
                    throw new NotImplementedException();
            }
        }

        protected SqlElement MakeSqlElement(Expression exp)
        {
            switch (exp)
            {
                case ParameterExpression pe:
                    return new SqlParameterElement(pe.Type, pe.Name);
                case ConstantExpression ce:
                    return new SqlConstantElement(ce.Value);
                case MemberExpression me:
                    return new SqlLeafElement(me.Member.GetColumnName());
                default:
                    throw new NotImplementedException();
            }
        }

        protected SqlElement MakeSqlElement(ExpressionType type, SqlElement x, SqlElement y)
        {
            switch (type)
            {
                case ExpressionType.AndAlso:
                    return new SqlBinaryElement(SqlElementVerbs.And, x, y);

                case ExpressionType.NotEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Ne, x, y);

                case ExpressionType.Equal:
                    return new SqlBinaryElement(SqlElementVerbs.Eq, x, y);

                case ExpressionType.GreaterThan:
                    return new SqlBinaryElement(SqlElementVerbs.Gt, x, y);

                case ExpressionType.GreaterThanOrEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Gte, x, y);

                case ExpressionType.LessThan:
                    return new SqlBinaryElement(SqlElementVerbs.Lt, x, y);

                case ExpressionType.LessThanOrEqual:
                    return new SqlBinaryElement(SqlElementVerbs.Lte, x, y);

                case ExpressionType.MemberAccess:
                    return new SqlBinaryElement(SqlElementVerbs.MemberAccess, x, y);

                case ExpressionType.Lambda:
                    return new SqlBinaryElement(SqlElementVerbs.Lambda, x, y);

                default:
                    throw new NotImplementedException();
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
        public string PrimaryKey = null;
        public string ForeignKey = null;
        public string[] Parameters { get; set; } = null;

        public Type PrimaryType = null;
        public string PrimaryParamName { get; set; } = null;
        public Type ForeignType = null;
        public string ForeignParamName { get; set; }

        public Func<dynamic, dynamic, bool> Conditions = null;
        public string Comparator = null;

        public MemberInfo MemberInfo { get; set; } = null;

        public BindingSpec()
        {
        }

        public BindingSpec(string fk, string pk)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            Comparator = "=";
            Conditions = MakeDefaultFunc(ForeignKey, PrimaryKey);
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

        private static dynamic Evaluate(BindingConditionElement el, ConditionContext context)
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

        private static (Func<dynamic, dynamic>, Type, string, string[]) MakeConditionFunc_(Expression exp)
        {
            // Func<dynamic, dynamic, dynamic> memberAccess = (x, y) => new object[] { x, y };

            switch (exp)
            {
                case LambdaExpression lme:
                    return MakeConditionFunc_(lme.Body);

                case MemberExpression me:
                    var (f, t, n, path) = MakeConditionFunc_(me.Expression);
                    var member = me.Member;
                    return ((x) => new BindingConditionElement(f(x), member), t, n, path.Concat(new string[] { member.Name }).ToArray());

                case ParameterExpression pe:
                    return ((x) => x, pe.Type, pe.Name, new string[] { });

                default:
                    throw new NotImplementedException();
            }
        }


        public static BindingSpec New(Type t, Expression exp, ConditionContext context = null)
        {
            Func<Func<dynamic, dynamic>, Func<dynamic, dynamic>, Func<dynamic, dynamic, bool>, Func<dynamic, dynamic, bool>> conditionFuncMaker = (Func<dynamic, dynamic> f, Func<dynamic, dynamic> g, Func<dynamic, dynamic, bool> ev) => (dynamic x, dynamic y) => ev(f(x), g(y)); // propertyGetter(defaultType, primaryKey)(l) == dictionaryGetter(foreignKey)(r);
            if (exp == null)
            {
                return null;
            }

            if (context == null)
            {
                context = new ConditionContext();
            }

            switch (exp)
            {
                case LambdaExpression lme:
                    context.PrimaryType = t;
                    context.Parameters = lme.Parameters.Select(x => x.Name).ToArray();
                    return New(t, lme.Body, context);

                case BinaryExpression bine:
                    Func<Func<dynamic, dynamic, bool>, dynamic, dynamic, bool> evaluator = (f, x, y) => f(Evaluate(x, context), Evaluate(y, context));

                    var (le, lty, ls, lpath) = MakeConditionFunc_(bine.Left);
                    var (re, rty, rs, rpath) = MakeConditionFunc_(bine.Right);

                    var fs = new Func<dynamic, dynamic>[context.Parameters.Length];
                    fs[Array.IndexOf(context.Parameters, ls)] = le;
                    fs[Array.IndexOf(context.Parameters, rs)] = re;

                    var primaryKey = StringUtils.ToSnake(string.Join("_", lty == context.PrimaryType ? lpath : rpath));
                    var foreignKey = StringUtils.ToSnake(string.Join("_", lty != context.PrimaryType ? lpath : rpath));

                    switch (bine.NodeType)
                    {
                        case ExpressionType.Equal:
                            Func<dynamic, dynamic, bool> eq = (x, y) => x == y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = "=", Conditions = (x, y) => evaluator(eq, fs[0](x), fs[1](y)) };

                        case ExpressionType.NotEqual:
                            Func<dynamic, dynamic, bool> ne = (x, y) => x != y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = "!=", Conditions = (x, y) => evaluator(ne, fs[0](x), fs[1](y)) };

                        case ExpressionType.LessThan:
                            Func<dynamic, dynamic, bool> lt = (x, y) => x < y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = "<", Conditions = (x, y) => evaluator(lt, fs[0](x), fs[1](y)) };

                        case ExpressionType.LessThanOrEqual:
                            Func<dynamic, dynamic, bool> lte = (x, y) => x <= y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = "<=", Conditions = (x, y) => evaluator(lte, fs[0](x), fs[1](y)) };

                        case ExpressionType.GreaterThan:
                            Func<dynamic, dynamic, bool> gt = (x, y) => x > y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = ">", Conditions = (x, y) => evaluator(gt, fs[0](x), fs[1](y)) };

                        case ExpressionType.GreaterThanOrEqual:
                            Func<dynamic, dynamic, bool> gte = (x, y) => x >= y;
                            return new BindingSpec() { PrimaryKey = primaryKey, ForeignKey = foreignKey, Comparator = ">=", Conditions = (x, y) => evaluator(gte, fs[0](x), fs[1](y)) };
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

            var bindingSpec = BindingSpec.New(memberInfo.DeclaringType, condexp);
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
            public Type PrimaryType = null;
            public string[] Parameters = null;
            public MemberInfo MemberInfo = null;
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

    public interface IFilterable<T> : IEnumerable<T> where T : class, new()
    {
        IFilterable<T> And(Expression<Func<T, bool>> cond);

        IFilterable<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new();
    }

    public class SelectStatement<T> : SqlSelectStatement, IFilterable<T>, IEnumerable<T> where T : class, new()
    {
        public Func<SelectStatement<T>, IEnumerable<T>> OnExecute { get; set; } = null;

        public SelectStatement() : base()
        {
        }
        public virtual IFilterable<T> And(Expression<Func<T, bool>> cond)
        {
            this.Root = BuildElement(this.Root, cond);
            return this;
        }

        public virtual IFilterable<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            var lambdaexp = (propexp as LambdaExpression).Body;
            var paramexp = (propexp as LambdaExpression).Parameters[0] as ParameterExpression;
            var primaryName = paramexp.Name;
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


        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return OnExecute(this).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return OnExecute(this).GetEnumerator();
        }

        // private SqlElement _conditions = null;
        // public override SqlElement Conditions { get => _conditions; }
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
    }

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

}
