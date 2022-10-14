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
        private RelationSpec(string fk, string pk, EvaluatorSpec? conditionEvaluatorSpec, Parameter left, Parameter right, MemberInfo? memberInfo)
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

            Left = left;
            Right = right;
            MemberInfo = memberInfo;
        }

        public string PrimaryKey { get; }
        public string ForeignKey { get; }

        public Parameter Left { get; }

        public Parameter Right { get; }

        public string ConditionSql => GetConditionSql();

        public Func<IDictionary<string, object?>, IDictionary<string, object?>, bool> ConditionFunc => ConditionEvaluatorSpec?.GetEvaluateFunc() ?? _defaultConditionFunc;
        private Func<object, object, bool> _defaultConditionFunc = default!;

        public EvaluatorSpec? ConditionEvaluatorSpec { get; }

        public MemberInfo? MemberInfo { get; }

        private string GetConditionSql() => $"{Left.Name}.{PrimaryKey ?? Left.Type.GetPrimaryKey()} = {Right.Name}.{ForeignKey}";

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

        public static RelationSpec Build(string leftName, Type leftType, string rightName, Type rightType, PropertyInfo? relationPropertyInfo, string? foreignKey_, LambdaExpression? condexp)
        {
            string primaryKey, foreignKey;
            EvaluatorSpec? evaluatorSpec = null;

            if (rightType.IsGenericType)
            {
                rightType = rightType.GetGenericArguments()[0];
            }

            if (condexp != null)
            {
                var relationCond = Condition.New(leftType, condexp.Body);

                primaryKey = string.Join("_", relationCond.Left.PathComponents).ToSnake();
                foreignKey = string.Join("_", relationCond.Right.PathComponents).ToSnake();

                if (string.IsNullOrEmpty(primaryKey))
                {
                    var leftpk = leftType.GetPrimaryKey();
                    if (leftpk == null)
                    {
                        throw new NotImplementedException();
                    }
                    primaryKey = leftpk;
                }
                foreignKey = rightType.GetForeignKey(foreignKey, leftType, primaryKey);

                evaluatorSpec = new EvaluatorSpec(relationCond.Comparator, relationCond.Left.Func, relationCond.Right.Func);
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
                    foreignKey = fki.ForeignKey;
                    primaryKey = fki.PrimaryKey ?? "id";
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

                    foreignKey = matchedColumnInfo.Name.ToLower();
                    primaryKey = matchedColumnInfo.BindingToPrimaryKey.ToLower();
                }
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

                foreignKey = matchedColumnInfo.Name.ToLower();
                primaryKey = matchedColumnInfo.BindingToPrimaryKey.ToLower();
            }

            var leftParameter = new Parameter(leftType, leftName);
            var rightParameter = new Parameter(rightType, rightName);

            return new RelationSpec(foreignKey, primaryKey, evaluatorSpec, leftParameter, rightParameter, relationPropertyInfo);
        }

        public class Parameter
        {
            public Parameter(Type type, string name)
            {
                Type = type;
                Name = name;
            }

            public Type Type { get; }

            public string Name { get; }
        }

        public class EvaluatorSpec
        {
            public EvaluatorSpec(Func<object?, object?, bool> comparator, Func<object, object?> left, Func<object, object?> right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }

            public Func<object?, object?, bool> Comparator { get; }
            public Func<object, object?> Left { get; }
            public Func<object, object?> Right { get; }


            public Func<object?, object?, bool> GetEvaluateFunc()
            {
                Func<Func<object?, object?, bool>, object?, object?, bool> evaluator = (f, xx, yy) =>
                {
                    return f(Evaluate(xx), Evaluate(yy));
                };

                return (x, y) => evaluator(Comparator, x != null ? Left(x) : null, y != null ? Right(y) : null);
            }

            private static object? Evaluate(object? o)
            {
                if (!(o is Condition.Node.Element el))
                {
                    throw new NotImplementedException();
                }

                var arr = el.Extend();
                var obj = arr.First();
                var propkeys = arr.Skip(1).Select(x => ((MemberInfo)x).Name);
                if (!propkeys.Any())
                {
                    propkeys = new[] { "id" };
                }
                var prop = string.Join("_", propkeys);

                return DictOrPropertyGet(obj, prop);
            }

            private static object? DictOrPropertyGet(object? x, string memb)
            {
                var dx = x as IDictionary<string, object?>;
                if (dx == null)
                {
                    return null;
                }

                var value = ReflectionHelpers.DictionaryGet(dx, memb);
                if (value != null)
                {
                    return value;
                }
                var obj = dx["__object"];
                return obj != null ? ReflectionHelpers.PropertyGet(obj, memb) : null;
            }
        }

        public class Condition
        {
            public Condition(Func<object?, object?, bool> comparator, Node left, Node right)
            {
                Comparator = comparator;
                Left = left;
                Right = right;
            }

            public Func<object?, object?, bool> Comparator { get; }

            public Node Left { get; }
            public Node Right { get; }

            public static Condition New(Type t, Expression exp)
            {
                switch (exp)
                {
                    case BinaryExpression bine:
                        var left = Node.New(bine.Left);
                        var right = Node.New(bine.Right);

                        if (left.Parameter.Type != t)
                        {
                            var x = left;
                            left = right;
                            right = x;
                        }

                        var px = Expression.Parameter(typeof(object), "x");
                        var py = Expression.Parameter(typeof(object), "y");

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

                        Func<object?, object?, bool> comparator = (Func<object?, object?, bool>)Expression.Lambda(bine2, px, py).Compile();
                        return new Condition(comparator, left, right);
                        // break;
                }

                throw new NotImplementedException();
            }

            public class Node
            {
                private Node(Func<object, Element> f, ParameterExpression pe, string[] comps)
                {
                    Func = f;
                    Parameter = pe;
                    PathComponents = comps;
                }

                public Func<object, Element> Func { get; }
                public ParameterExpression Parameter { get; }
                public string[] PathComponents { get; }

                public static Node New(Expression exp)
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
                            var newPathComponents = entry.PathComponents.Concat(new[] { propertyInfo.Name });
                            return new Node(x => new Element(entry.Func(x), propertyInfo), entry.Parameter, newPathComponents.ToArray());

                        case ParameterExpression pe:
                            return new Node(x => new Element(x, null), pe, Array.Empty<string>());
                    }

                    throw new NotImplementedException();
                }

                public class Element
                {
                    public Element(object? l, object? r)
                    {
                        Left = l;
                        Right = r;
                    }

                    public object? Left { get; }
                    public object? Right { get; }

                    public object[] Extend()
                    {
                        var ret = new List<object>();

                        if (Left is Element le)
                        {
                            ret.AddRange(le.Extend());
                        }
                        else if (Left != null)
                        {
                            ret.Add(Left);
                        }

                        if (Right is Element re)
                        {
                            ret.AddRange(re.Extend());
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
}
