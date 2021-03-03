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
                    var foreignTableInfo = t.GetTableInfoFromType();
                    var matched = foreignTableInfo.Where(x => x.BindingTo == primaryTable).First();
                    bindingSpec = new BindingSpec(matched.ColumnName.ToLower(), matched.BindingToPrimaryKey.ToLower());
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

    public class SelectStatement<T> : SqlSelectStatement, IEnumerable<T> where T : class
    {
        public Func<SelectStatement<T>, IEnumerable<T>> OnExecute = null;

        public SelectStatement() : base()
        {
        }

        public virtual SelectStatement<T> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> propexp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            var lambdaexp = (propexp as LambdaExpression).Body;
            var paramexp = (propexp as LambdaExpression).Parameters[0] as ParameterExpression;
            var primaryName = paramexp.Name;
            var memberInfo = (lambdaexp as MemberExpression).Member;

            var bindingSpec = BindingSpec.Build(paramexp.Type, paramexp.Name, memberInfo, "t", cond);
            RegisterBindingSpec(bindingSpec);
            return this;
        }

        public void RegisterBindingSpec(BindingSpec bindingSpec)
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

#if false
    public class JoinedSelectStatement<JT, T, T1> : SelectStatement<T>
        where JT : Joined<T, T1>
        where T : class
        where T1 : class
    {
        // public string ForeignKey = null;
        // public string PrimaryKey = null;

        public JoinedSelectStatement(SqlElement newroot) : base(newroot)
        {
        }

        /*protected override JoinedSelectStatement<Joined<T, T2>, T, T2> BuildJoins<T2>(MemberInfo memberInfo, Expression<Func<T, T2, bool>> cond) where T2 : class
        {
            return new JoinedSelectStatement<Joined<T, T2>, T, T2>(this.Root, memberInfo, cond) { OnExecute = this.OnExecute };
        }
        */

    }
#endif



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
