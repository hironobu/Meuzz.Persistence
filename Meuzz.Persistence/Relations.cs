using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;

namespace Meuzz.Persistence
{
    public class RelationSpec
    {
        public string PrimaryKey { get; set; } = null;
        public string ForeignKey { get; set; } = null;

        public Parameter Primary { get; set; }

        public Parameter Foreign { get; set; }

        public string ConditionSql { get => GetConditionSql(); }

        public Func<dynamic, dynamic, bool> ConditionFunc
        {
            get 
            {
                if (ConditionEvaluatorSpec == null)
                {
                    return _defaultConditionFunc;
                }

                return ConditionEvaluatorSpec.GetEvaluateFunc();
            }
        }
        private Func<dynamic, dynamic, bool> _defaultConditionFunc = null;

        public EvaluatorSpec ConditionEvaluatorSpec { get; set; } = null;

        public MemberInfo MemberInfo { get; set; } = null;

        public RelationSpec()
        {
        }

        public RelationSpec(string fk, string pk)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            _defaultConditionFunc = MakeDefaultFunc(ForeignKey, PrimaryKey);
        }

        private string GetConditionSql()
        {
            return $"{Primary.Name}.{PrimaryKey ?? Primary.Type.GetPrimaryKey()} {"="} {Foreign.Name}.{ForeignKey}";
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

        public static RelationSpec Build(Type primaryType, string primaryName, MemberInfo memberInfo, string defaultForeignParamName, Expression condexp)
        {
            var propinfo = (memberInfo.MemberType == MemberTypes.Property) ? (memberInfo as PropertyInfo) : null;
            var foreignType = propinfo.PropertyType;
            if (foreignType.IsGenericType)
            {
                foreignType = foreignType.GetGenericArguments()[0];
            }

            RelationSpec joiningSpec = null;
            if (condexp != null)
            {
                if (!(condexp is LambdaExpression lme))
                {
                    throw new NotImplementedException();
                }

                var bindingParams = Condition.New(memberInfo.DeclaringType, lme.Body);

                var primaryKey = StringUtils.ToSnake(string.Join("_", bindingParams.Left.path));
                var foreignKey = StringUtils.ToSnake(string.Join("_", bindingParams.Right.path));

                primaryKey = string.IsNullOrEmpty(primaryKey) ? primaryType.GetPrimaryKey() : primaryKey;
                foreignKey = foreignType.GetForeignKey(foreignKey, primaryType, primaryKey);

                joiningSpec = new RelationSpec()
                {
                    PrimaryKey = primaryKey,
                    ForeignKey = foreignKey,
                    ConditionEvaluatorSpec = new EvaluatorSpec(bindingParams.Comparator, bindingParams.Left.f, bindingParams.Right.f)
                };
            }

            if (joiningSpec == null)
            {
                var fki = ForeignKeyInfoManager.Instance().GetForeignKeyInfoByPropertyInfo(propinfo);
                if (fki != null)
                {
                    joiningSpec = new RelationSpec(fki.ForeignKey, fki.PrimaryKey ?? "id");
                }
                else
                {
                    var primaryTable = memberInfo.DeclaringType.GetTableName();
                    var foreignClassInfo = foreignType.GetClassInfo();
                    var matched = foreignClassInfo.Columns.Where(x => x.BindingTo == primaryTable).First();
                    joiningSpec = new RelationSpec(matched.Name.ToLower(), matched.BindingToPrimaryKey.ToLower());
                }
            }

            joiningSpec.Primary = new Parameter() { Type = primaryType, Name = primaryName };
            joiningSpec.Foreign = new Parameter() { Type = foreignType, Name = defaultForeignParamName };
            joiningSpec.MemberInfo = memberInfo;

            return joiningSpec;
        }

        public class Parameter
        {
            public Type Type { get; set; }

            public string Name { get; set; }
        }

        public class EvaluatorSpec
        {
            public Func<dynamic, dynamic, bool> Comparator { get; set; } = null;
            public dynamic Left { get; set; } = null;
            public dynamic Right { get; set; } = null;

            public EvaluatorSpec(Func<dynamic, dynamic, bool> comparator, dynamic left, dynamic right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }

            private static dynamic Evaluate(object o)
            {
                if (!(o is Condition.Element el))
                {
                    throw new NotImplementedException();
                }

                Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(StringUtils.ToCamel(prop, true)).GetValue(x);
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

            public Func<dynamic, dynamic, bool> GetEvaluateFunc()
            {
                Func<Func<dynamic, dynamic, bool>, dynamic, dynamic, bool> evaluator = (f, xx, yy) =>
                {
                    return f(Evaluate(xx), Evaluate(yy));
                };

                return (x, y) => evaluator(Comparator, Left(x), Right(y));
            }
        }

        public class Condition
        {
            public Func<dynamic, dynamic, bool> Comparator { get; set; } = null;
            public Entry Left { get; set; } = null;
            public Entry Right { get; set; } = null;

            public Condition(Func<dynamic, dynamic, bool> comparator, Entry left, Entry right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }

            public static Condition New(Type t, Expression exp)
            {
                if (exp == null)
                {
                    throw new NotImplementedException();
                }

                switch (exp)
                {
                    case BinaryExpression bine:
                        var left = Entry.New(bine.Left);
                        var right = Entry.New(bine.Right);

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
                                return new Condition(eq, left, right);

                            case ExpressionType.NotEqual:
                                Func<dynamic, dynamic, bool> ne = (x, y) => x != y;
                                return new Condition(ne, left, right);

                            case ExpressionType.LessThan:
                                Func<dynamic, dynamic, bool> lt = (x, y) => x < y;
                                return new Condition(lt, left, right);

                            case ExpressionType.LessThanOrEqual:
                                Func<dynamic, dynamic, bool> lte = (x, y) => x <= y;
                                return new Condition(lte, left, right);

                            case ExpressionType.GreaterThan:
                                Func<dynamic, dynamic, bool> gt = (x, y) => x > y;
                                return new Condition(gt, left, right);

                            case ExpressionType.GreaterThanOrEqual:
                                Func<dynamic, dynamic, bool> gte = (x, y) => x >= y;
                                return new Condition(gte, left, right);
                        }
                        break;
                }

                throw new NotImplementedException();
            }

            public class Entry
            {
                public Func<dynamic, dynamic> f { get; set; } = null;
                public ParameterExpression e { get; set; } = null;

                public string[] path { get; set; } = null;

                public static Entry New(Expression exp)
                {
                    switch (exp)
                    {
                        case MemberExpression me:
                            var entry = New(me.Expression);
                            var propertyInfo = me.Member as PropertyInfo;
                            var newPath = entry.path.Concat(new string[] { propertyInfo.Name });
                            return new Entry() { f = (x) => new Element(entry.f(x), propertyInfo), e = entry.e, path = newPath.ToArray() };

                        case ParameterExpression pe:
                            return new Entry() { f = (x) => new Element(x, null), e = pe, path = new string[] { } };
                    }

                    throw new NotImplementedException();
                }
            }

            public class Element
            {
                public dynamic Left;
                public dynamic Right;

                public Element(dynamic l, dynamic r)
                {
                    this.Left = l;
                    this.Right = r;
                }

                public dynamic[] Evaluate()
                {
                    var ret = new List<dynamic>();

                    if (Left is Element)
                    {
                        ret.AddRange(Left.Evaluate());
                    }
                    else if (Left != null)
                    {
                        ret.Add(Left);
                    }

                    if (Right is Element)
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
}
