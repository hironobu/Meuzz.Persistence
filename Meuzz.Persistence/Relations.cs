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
        private RelationSpec(string fk, string pk, Parameter left, Parameter right, MemberInfo? memberInfo, Condition? condition)
        {
            ForeignKey = fk;
            PrimaryKey = pk ?? "id";
            Left = left;
            Right = right;
            MemberInfo = memberInfo;

            _conditionFunc = condition?.GetEvaluateFunc() ?? MakeDefaultConditionFunc(ForeignKey, PrimaryKey);
        }

        public string PrimaryKey { get; }
        public string ForeignKey { get; }
        public Parameter Left { get; }
        public Parameter Right { get; }
        public MemberInfo? MemberInfo { get; }

        public string ConditionSql => $"{Left.Name}.{PrimaryKey ?? Left.Type.GetPrimaryKey()} = {Right.Name}.{ForeignKey}";
        public Func<ValueObjectComposite, ValueObjectComposite, bool> ConditionFunc => _conditionFunc;

        private Func<ValueObjectComposite, ValueObjectComposite, bool> _conditionFunc;

        public static RelationSpec Build(string leftName, Type leftType, string rightName, Type rightType, PropertyInfo? relationPropertyInfo, LambdaExpression? condexp)
        {
            string primaryKey, foreignKey;
            Condition? condition = null;

            if (rightType.IsGenericType)
            {
                rightType = rightType.GetGenericArguments()[0];
            }

            if (condexp != null)
            {
                condition = Condition.New(leftType, condexp.Body);

                primaryKey = string.Join("_", condition.Left.KeyPath).ToSnake();
                foreignKey = string.Join("_", condition.Right.KeyPath).ToSnake();

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
            }
            else if (relationPropertyInfo != null)
            {
                var fki = relationPropertyInfo.GetForeignKeyInfo();
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

            return new RelationSpec(foreignKey, primaryKey, leftParameter, rightParameter, relationPropertyInfo, condition);
        }

        private static Func<ValueObjectComposite, ValueObjectComposite, bool> MakeDefaultConditionFunc(string foreignKey, string primaryKey)
        {
            Func<Func<object?, object?, bool>, Func<ValueObjectComposite, object?>, Func<ValueObjectComposite, object?>, Func<ValueObjectComposite, ValueObjectComposite, bool>> joiningConditionMaker
                = (Func<object?, object?, bool> eval, Func<ValueObjectComposite, object?> f, Func<ValueObjectComposite, object?> g) => (ValueObjectComposite x, ValueObjectComposite y) => eval(f(x), g(y));
            Func<string, Func<ValueObjectComposite, object?>> memberAccessor = (string memb) => (ValueObjectComposite x) => x.KeyPathGet(memb);

            return joiningConditionMaker((x, y) => x == y, memberAccessor(primaryKey), memberAccessor(foreignKey));
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

            public Func<ValueObjectComposite, ValueObjectComposite, bool> GetEvaluateFunc() => (x, y) => Comparator(UnpackElement(Left.Func(x)), UnpackElement(Right.Func(y)));

            private object? UnpackElement(Node.Element? el)
            {
                if (el == null)
                {
                    return null;
                }

                var arr = el.Extend();
                var obj = arr.First();
                var propkeys = arr.Skip(1).Select(x => ((MemberInfo)x).Name);
                if (!propkeys.Any())
                {
                    propkeys = new[] { "id" };
                }
                var prokeyp = string.Join("_", propkeys);

                var dx = obj as ValueObjectComposite;
                if (dx == null)
                {
                    return null;
                }

                return dx.KeyPathGet(prokeyp);
            }

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
                private Node(Func<ValueObjectComposite, Element> f, ParameterExpression pe, string[] comps)
                {
                    Func = f;
                    Parameter = pe;
                    KeyPath = comps;
                }

                public Func<ValueObjectComposite, Element> Func { get; }
                public ParameterExpression Parameter { get; }
                public string[] KeyPath { get; }

                public static Node New(Expression exp)
                {
                    switch (exp)
                    {
                        case MemberExpression me:
                            var entry = New(me.Expression);
                            var newPathComponents = entry.KeyPath.Concat(new[] { me.Member.Name });
                            return new Node(x => new Element(entry.Func(x), me.Member), entry.Parameter, newPathComponents.ToArray());

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
