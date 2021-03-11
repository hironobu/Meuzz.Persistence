using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class BindingConditionEvaluatorParams
    {
        public Func<dynamic, dynamic, bool> Comparator { get; set; } = null;
        public dynamic Left { get; set; } = null;
        public dynamic Right { get; set; } = null;

        public BindingConditionEvaluatorParams(Func<dynamic, dynamic, bool> comparator, dynamic left, dynamic right)
        {
            Comparator = comparator;
            Left = left;
            Right = right;
        }
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

        public BindingConditionEvaluatorParams ConditionParams { get; set; } = null;

        public MemberInfo MemberInfo { get; set; } = null;

        public BindingSpec()
        {
        }

        public BindingSpec(string fk, string pk)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            _defaultConditionFunc = MakeDefaultFunc(ForeignKey, PrimaryKey);
        }


        private string GetConditionSql()
        {
            return $"{PrimaryParamName}.{PrimaryKey ?? PrimaryType.GetPrimaryKey()} {"="} {ForeignParamName}.{ForeignKey}";
        }

        private Func<dynamic, dynamic, bool> GetConditionFunc()
        {
            Func<Func<dynamic, dynamic, bool>, dynamic, dynamic, bool> evaluator = (f, xx, yy) =>
            {
                return f(Evaluate(xx), Evaluate(yy));
            };

            return (x, y) => evaluator(ConditionParams.Comparator, ConditionParams.Left(x), ConditionParams.Right(y));
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

        private static dynamic Evaluate(object o)
        {
            if (!(o is BindingConditionElement el))
            {
                throw new NotImplementedException();
            }

            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
            // Func<dynamic, string, dynamic> dictionaryGetter = (dynamic x, string key) => x[StringUtils.ToSnake(key)];
            /*
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
            }*/
            var arr = el.Evaluate().ToArray();
            var obj = arr.First();
            var propkeys = arr.Skip(1).Select(x => x.Name);
            if (propkeys.Count() == 0)
            {
                propkeys = new string[] { "id" };
            }
            var prop = string.Join("_", propkeys);

            Func<string, Func<dynamic, dynamic>> memberAccessor = (string memb) => (dynamic x) =>
            {
                var col = StringUtils.ToSnake(memb);
                if (x.ContainsKey(col))
                {
                    return x[col];
                }
                if (col != "id" && x.ContainsKey(col + "_id"))
                {
                    return x[col + "_id"];
                }
                return propertyGetter(memb)(x["__object"]);
            };

            return memberAccessor(prop)(obj);
        }

        public class BindingConditionEntry
        {
            public Func<dynamic, dynamic> f { get; set; } = null;
            public ParameterExpression e { get; set; } = null;

            public string[] path { get; set; } = null;
        }

        private static BindingConditionEntry NewConditionEntry(Expression exp)
        {
            switch (exp)
            {
                case MemberExpression me:
                    var entry = NewConditionEntry(me.Expression);
                    var propertyInfo = me.Member as PropertyInfo;
                    var newPath = entry.path.Concat(new string[] { propertyInfo.Name });
                    return new BindingConditionEntry() { f = (x) => new BindingConditionElement(entry.f(x), propertyInfo), e = entry.e, path = newPath.ToArray() };

                case ParameterExpression pe:
                    return new BindingConditionEntry() { f = (x) => new BindingConditionElement(x, null), e = pe, path = new string[] { } };

            }

            throw new NotImplementedException();
        }

        public class BindingCondition
        {
            public Func<dynamic, dynamic, bool> Comparator { get; set; } = null;
            public BindingConditionEntry Left { get; set; } = null;
            public BindingConditionEntry Right { get; set; } = null;

            public BindingCondition(Func<dynamic, dynamic, bool> comparator, BindingConditionEntry left, BindingConditionEntry right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }
        }

        public static BindingCondition New(Type t, Expression exp)
        {
            if (exp == null)
            {
                throw new NotImplementedException();
            }

            switch (exp)
            {
                case BinaryExpression bine:
                    var left = NewConditionEntry(bine.Left);
                    var right = NewConditionEntry(bine.Right);

                    if (left.e.Type != t)
                    {
                        var x = left;
                        left = right;
                        right = x;
                    }

                    switch (bine.NodeType)
                    {
                        case ExpressionType.Equal:
                            Func<dynamic, dynamic, bool> eq = (x, y) => x == y;
                            return new BindingCondition(eq, left, right);

                        case ExpressionType.NotEqual:
                            Func<dynamic, dynamic, bool> ne = (x, y) => x != y;
                            return new BindingCondition(ne, left, right);

                        case ExpressionType.LessThan:
                            Func<dynamic, dynamic, bool> lt = (x, y) => x < y;
                            return new BindingCondition(lt, left, right);

                        case ExpressionType.LessThanOrEqual:
                            Func<dynamic, dynamic, bool> lte = (x, y) => x <= y;
                            return new BindingCondition(lte, left, right);

                        case ExpressionType.GreaterThan:
                            Func<dynamic, dynamic, bool> gt = (x, y) => x > y;
                            return new BindingCondition(gt, left, right);

                        case ExpressionType.GreaterThanOrEqual:
                            Func<dynamic, dynamic, bool> gte = (x, y) => x >= y;
                            return new BindingCondition(gte, left, right);
                    }
                    break;
            }

            throw new NotImplementedException();
        }

        public static BindingSpec Build(Type primaryType, string primaryName, MemberInfo memberInfo, string defaultForeignParamName, Expression condexp)
        {
            var propinfo = (memberInfo.MemberType == MemberTypes.Property) ? (memberInfo as PropertyInfo) : null;
            var foreignType = propinfo.PropertyType;
            if (foreignType.IsGenericType)
            {
                foreignType = foreignType.GetGenericArguments()[0];
            }

            BindingSpec bindingSpec = null;
            if (condexp != null)
            {
                if (!(condexp is LambdaExpression lme))
                {
                    throw new NotImplementedException();
                }

                // var parameters = lme.Parameters.Select(x => x.Name).ToArray();

                var bindingParams = BindingSpec.New(memberInfo.DeclaringType, lme.Body);

                var primaryKey = StringUtils.ToSnake(string.Join("_", bindingParams.Left.path));
                var foreignKey = StringUtils.ToSnake(string.Join("_", bindingParams.Right.path));

                primaryKey = string.IsNullOrEmpty(primaryKey) ? primaryType.GetPrimaryKey() : primaryKey;
                foreignKey = foreignType.GetForeignKey(foreignKey, primaryType, primaryKey);

                bindingSpec = new BindingSpec()
                {
                    PrimaryKey = primaryKey,
                    ForeignKey = foreignKey,
                    ConditionParams = new BindingConditionEvaluatorParams(bindingParams.Comparator, bindingParams.Left.f, bindingParams.Right.f)
                };
            }

            if (bindingSpec == null)
            {
                var fki = propinfo.GetForeignKeyInfo();
                if (fki != null)
                {
                    bindingSpec = new BindingSpec(fki.ForeignKey, fki.PrimaryKey ?? "id");
                }
                else
                {
                    var primaryTable = memberInfo.DeclaringType.GetTableName();
                    var foreignTableInfo = foreignType.GetTableInfo();
                    var matched = foreignTableInfo.Columns.Where(x => x.BindingTo == primaryTable).First();
                    bindingSpec = new BindingSpec(matched.Name.ToLower(), matched.BindingToPrimaryKey.ToLower());
                }
            }

            bindingSpec.PrimaryType = primaryType;
            bindingSpec.PrimaryParamName = primaryName;
            bindingSpec.ForeignType = foreignType;
            bindingSpec.ForeignParamName = defaultForeignParamName;
            bindingSpec.MemberInfo = memberInfo;

            return bindingSpec;
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
                else if (Left != null)
                {
                    ret.Add(Left);
                }
                if (Right is BindingConditionElement)
                {
                    ret.AddRange(Right.Evaluate());
                }
                else if (Right != null)
                {
                    ret.Add(Right);
                }

                return ret.ToArray();
            }
        }
    }

}
