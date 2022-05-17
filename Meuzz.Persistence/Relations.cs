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

        public Func<IDictionary<string, object?>, IDictionary<string, object?>, bool> ConditionFunc
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
        private Func<object, object, bool> _defaultConditionFunc = default!;

        public EvaluatorSpec? ConditionEvaluatorSpec { get; } = null;

        public MemberInfo? MemberInfo { get; set; } = null;

        public RelationSpec(string fk, string pk, EvaluatorSpec? conditionEvaluatorSpec = null)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            if (conditionEvaluatorSpec != null)
            {
                ConditionEvaluatorSpec = conditionEvaluatorSpec;
            }
            else
            {
                _defaultConditionFunc = MakeDefaultConditionFunc(ForeignKey, PrimaryKey);
            }
        }

        private string GetConditionSql()
        {
            return $"{Left.Name}.{PrimaryKey ?? Left.Type.GetPrimaryKey()} = {Right.Name}.{ForeignKey}";
        }

        private static Func<object, object, bool> MakeDefaultConditionFunc(string foreignKey, string primaryKey)
        {
            Func<Func<object?, object?, bool>, Func<object, object?>, Func<object, object?>, Func<object, object, bool>> joiningConditionMaker
                = (Func<object?, object?, bool> eval, Func<object, object?> f, Func<object, object?> g) => (object x, object y) => eval(f(x), g(y));
            Func<object?, object?, bool> eq = (x, y) => x == y;
            Func<string, Func<object, object?>> memberAccessor = (string memb) => (object x) =>
            {
                if (x is IDictionary<string, object?> dx)
                {
                    return dx[memb];
                }
                else
                {
                    return ReflectionHelpers.PropertyGet(x, memb);
                }
            };

            return joiningConditionMaker(eq, memberAccessor(primaryKey), memberAccessor(foreignKey));
        }

        public static RelationSpec Build(string leftName, Type leftType, string rightName, Type rightType, PropertyInfo? relationPropertyInfo, LambdaExpression? condexp)
        {
            if (rightType.IsGenericType)
            {
                rightType = rightType.GetGenericArguments()[0];
            }

            RelationSpec? relationSpec = null;
            if (condexp != null)
            {
                var relationCond = Condition.New(leftType, condexp.Body);

                var primaryKey = string.Join("_", relationCond.Left.PathComponents).ToSnake();
                var foreignKey = string.Join("_", relationCond.Right.PathComponents).ToSnake();

                primaryKey = string.IsNullOrEmpty(primaryKey) ? leftType.GetPrimaryKey() : primaryKey;
                if (primaryKey == null)
                {
                    throw new NotImplementedException();
                }
                foreignKey = rightType.GetForeignKey(foreignKey, leftType, primaryKey);

                relationSpec = new RelationSpec(foreignKey, primaryKey, new EvaluatorSpec(relationCond.Comparator, relationCond.Left.f, relationCond.Right.f));
            }
            else if (relationPropertyInfo != null)
            {
                var fki = ForeignKeyInfoManager.Instance().GetRelatedForeignKeyInfoByReferencingPropertyInfo(relationPropertyInfo);
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
                    var primaryTable = leftType.GetTableName();
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
            else
            {
                throw new ArgumentException("eigher relationPropertyInfo or condexp is should be non-null");
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
            public Func<object?, object?, bool> Comparator { get; set; } = default!;
            public Func<object, object?> Left { get; set; } = default!;
            public Func<object, object?> Right { get; set; } = default!;

            public EvaluatorSpec(Func<object?, object?, bool> comparator, Func<object, object?> left, Func<object, object?> right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }

            private static object? Evaluate(object? o)
            {
                if (!(o is Condition.Element el))
                {
                    throw new NotImplementedException();
                }

                var arr = el.Evaluate().ToArray();
                var obj = arr.First();
                var propkeys = arr.Skip(1).Select(x => ((MemberInfo)x).Name);
                if (!propkeys.Any())
                {
                    propkeys = new string[] { "id" };
                }
                var prop = string.Join("_", propkeys);

                return MemberGet(obj, prop);
            }

            public Func<object?, object?, bool> GetEvaluateFunc()
            {
                Func<Func<object?, object?, bool>, object?, object?, bool> evaluator = (f, xx, yy) =>
                {
                    return f(Evaluate(xx), Evaluate(yy));
                };

                return (x, y) => evaluator(Comparator, x != null ? Left(x) : null, y != null ? Right(y) : null);
            }

            private static object? MemberGet(object? x, string memb)
            {
                var dx = x as IDictionary<string, object?>;
                if (dx == null)
                {
                    return null;
                }

                var col = memb.ToSnake();
                if (dx.ContainsKey(col))
                {
                    return dx[col];
                }
                if (col != "id" && dx.ContainsKey(col + "_id"))
                {
                    return dx[col + "_id"];
                }
                var obj = dx["__object"];
                return obj != null ? ReflectionHelpers.PropertyGet(obj, memb) : null;
            }
        }

        public class Condition
        {
            public Func<object?, object?, bool> Comparator { get; set; } = default!;
            public Entry Left { get; set; } = default!;
            public Entry Right { get; set; } = default!;

            public Condition(Func<object?, object?, bool> comparator, Entry left, Entry right)
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

                        ParameterExpression px = Expression.Parameter(typeof(object), "x");
                        ParameterExpression py = Expression.Parameter(typeof(object), "y");

                        BinaryExpression? bine2;

                        switch (bine.NodeType)
                        {
                            case ExpressionType.Equal:
                                bine2 = Expression.Equal(px, py);
                                break;

                            case ExpressionType.NotEqual:
                                bine2 = Expression.NotEqual(px, py);
                                break;

                            case ExpressionType.LessThan:
                                bine2 = Expression.LessThan(px, py);
                                break;

                            case ExpressionType.LessThanOrEqual:
                                bine2 = Expression.LessThanOrEqual(px, py);
                                break;

                            case ExpressionType.GreaterThan:
                                bine2 = Expression.GreaterThan(px, py);
                                break;

                            case ExpressionType.GreaterThanOrEqual:
                                bine2 = Expression.GreaterThanOrEqual(px, py);
                                break;

                            default:
                                throw new NotImplementedException();
                        }

                        return new Condition((Func<object?, object?, bool>)Expression.Lambda(bine2, px, py).Compile(), left, right);
                        // break;
                }

                throw new NotImplementedException();
            }

            public class Entry
            {
                public Func<object, Element> f { get; set; } = default!;
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
                            return new Entry() { f = x => new Element(entry.f(x), propertyInfo), e = entry.e, PathComponents = newPath.ToArray() };

                        case ParameterExpression pe:
                            return new Entry() { f = x => new Element(x, null), e = pe, PathComponents = new string[] { } };
                    }

                    throw new NotImplementedException();
                }
            }

            public class Element
            {
                public object? Left;
                public object? Right;

                public Element(object? l, object? r)
                {
                    this.Left = l;
                    this.Right = r;
                }

                public object[] Evaluate()
                {
                    var ret = new List<object>();

                    if (Left is Element le)
                    {
                        ret.AddRange(le.Evaluate());
                    }
                    else if (Left != null)
                    {
                        ret.Add(Left);
                    }

                    if (Right is Element re)
                    {
                        ret.AddRange(re.Evaluate());
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
