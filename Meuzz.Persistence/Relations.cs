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
        public string PrimaryKey { get; set; } = default!;
        public string ForeignKey { get; set; } = default!;

        public Parameter Left { get; set; } = default!;

        public Parameter Right { get; set; } = default!;

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
        private Func<dynamic, dynamic, bool> _defaultConditionFunc = default!;

        public EvaluatorSpec ConditionEvaluatorSpec { get; set; } = default!;

        public MemberInfo MemberInfo { get; set; } = default!;

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
            return $"{Left.Name}.{PrimaryKey ?? Left.Type.GetPrimaryKey()} = {Right.Name}.{ForeignKey}";
        }

        private static Func<dynamic, dynamic, bool> MakeDefaultFunc(string foreignKey, string primaryKey)
        {
            Func<Func<dynamic, dynamic, bool>, Func<dynamic, dynamic>, Func<dynamic, dynamic>, Func<dynamic, dynamic, bool>> joiningConditionMaker
                = (Func<dynamic, dynamic, bool> eval, Func<dynamic, dynamic> f, Func<dynamic, dynamic> g) => (dynamic x, dynamic y) => eval(f(x), g(y));
            Func<dynamic, dynamic, bool> eq = (x, y) => x == y;
            Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(prop.ToCamel(true)).GetValue(x);
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

        public static RelationSpec Build(string leftName, Type leftType, string rightName, PropertyInfo relationPropertyInfo, LambdaExpression? condexp)
        {
            if (relationPropertyInfo.DeclaringType == null)
            {
                throw new ArgumentException("Invalid Parameter", "relationMemberInfo");
            }

            var rightType = relationPropertyInfo.PropertyType;
            if (rightType == null)
            {
                throw new NotImplementedException();
            }
            if (rightType.IsGenericType)
            {
                rightType = rightType.GetGenericArguments()[0];
            }

            RelationSpec? relationSpec = null;
            if (condexp != null)
            {
                var relationCond = Condition.New(relationPropertyInfo.DeclaringType, condexp.Body);

                var primaryKey = string.Join("_", relationCond.Left.PathComponents).ToSnake();
                var foreignKey = string.Join("_", relationCond.Right.PathComponents).ToSnake();

                primaryKey = string.IsNullOrEmpty(primaryKey) ? leftType.GetPrimaryKey() : primaryKey;
                if (primaryKey == null)
                {
                    throw new NotImplementedException();
                }
                foreignKey = rightType.GetForeignKey(foreignKey, leftType, primaryKey);

                relationSpec = new RelationSpec()
                {
                    PrimaryKey = primaryKey,
                    ForeignKey = foreignKey,
                    ConditionEvaluatorSpec = new EvaluatorSpec(relationCond.Comparator, relationCond.Left.f, relationCond.Right.f)
                };
            }
            else
            {
                var fki = ForeignKeyInfoManager.Instance().GetForeignKeyInfoByPropertyInfo(relationPropertyInfo);
                if (fki != null)
                {
                    if (fki.ForeignKey == null)
                    {
                        throw new NotImplementedException();
                    }
                    relationSpec = new RelationSpec(fki.ForeignKey, fki.PrimaryKey ?? "id");
                }
                else
                {
                    var primaryTable = relationPropertyInfo.DeclaringType.GetTableName();
                    var foreignTableInfo = rightType.GetTableInfo();
                    if (foreignTableInfo == null)
                    {
                        throw new NotImplementedException();
                    }
                    var matchedColumnInfo = foreignTableInfo.Columns.Where(x => x.BindingTo == primaryTable).First();
                    if (matchedColumnInfo.BindingToPrimaryKey == null)
                    {
                        throw new NotImplementedException();
                    }
                    relationSpec = new RelationSpec(matchedColumnInfo.Name.ToLower(), matchedColumnInfo.BindingToPrimaryKey.ToLower());
                }
            }

            relationSpec.Left = new Parameter() { Type = leftType, Name = leftName };
            relationSpec.Right = new Parameter() { Type = rightType, Name = rightName };
            relationSpec.MemberInfo = relationPropertyInfo;

            return relationSpec;
        }

        public class Parameter
        {
            public Type Type { get; set; } = default!;

            public string Name { get; set; } = default!;
        }

        public class EvaluatorSpec
        {
            public Func<dynamic, dynamic, bool> Comparator { get; set; } = default!;
            public dynamic Left { get; set; } = default!;
            public dynamic Right { get; set; } = default!;

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

                Func<string, Func<dynamic, dynamic>> propertyGetter = (string prop) => (dynamic x) => x.GetType().GetProperty(prop.ToCamel(true)).GetValue(x);
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
                    var col = memb.ToSnake();
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
            public Func<dynamic, dynamic, bool> Comparator { get; set; } = default!;
            public Entry Left { get; set; } = default!;
            public Entry Right { get; set; } = default!;

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
                public Func<dynamic, dynamic> f { get; set; } = default!;
                public ParameterExpression e { get; set; } = default!;

                public string[] PathComponents { get; set; } = default!;

                public static Entry New(Expression exp)
                {
                    switch (exp)
                    {
                        case MemberExpression me:
                            var entry = New(me.Expression);
                            var propertyInfo = me.Member as PropertyInfo;
                            if (propertyInfo == null)
                            {
                                throw new NotImplementedException();
                            }
                            var newPath = entry.PathComponents.Concat(new string[] { propertyInfo.Name });
                            return new Entry() { f = (x) => new Element(entry.f(x), propertyInfo), e = entry.e, PathComponents = newPath.ToArray() };

                        case ParameterExpression pe:
                            return new Entry() { f = (x) => new Element(x, null), e = pe, PathComponents = new string[] { } };
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
