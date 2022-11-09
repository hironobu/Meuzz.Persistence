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

                primaryKey = string.Join("_", condition.LeftKeyPath).ToSnake();
                foreignKey = string.Join("_", condition.RightKeyPath).ToSnake();

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

            return joiningConditionMaker((x, y) => x == y, x => x.KeyPathGet(primaryKey), y => y.KeyPathGet(foreignKey));
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
            private Condition(Func<ValueObjectComposite, ValueObjectComposite, bool> comparator, string[] left, string[] right)
            {
                Comparator = comparator;
                LeftKeyPath = left.Any() ? left : new[] { "id" };
                RightKeyPath = right.Any() ? right : new[] { "id" };
            }

            public Func<ValueObjectComposite, ValueObjectComposite, bool> Comparator { get; }

            public string[] LeftKeyPath { get; }
            public string[] RightKeyPath { get; }

            public Func<ValueObjectComposite, ValueObjectComposite, bool> GetEvaluateFunc() => (x, y) => Comparator(x, y);

            public static Condition New(Type t, Expression exp)
            {
                switch (exp)
                {
                    case BinaryExpression bine:
                        var px = Expression.Parameter(typeof(ValueObjectComposite), "x");
                        var py = Expression.Parameter(typeof(ValueObjectComposite), "y");

                        var (left, leftKeyPath) = MakeExpression(bine.Left, px);
                        var (right, rightKeyPath) = MakeExpression(bine.Right, py);

                        if (left.Type != t)
                        {
                            var x = left;
                            left = right;
                            right = x;
                        }

                        left = MakeUnpackComposite(left);
                        right = MakeUnpackComposite(right);

                        BinaryExpression? bine2;

                        switch (bine.NodeType)
                        {
                            case ExpressionType.Equal:
                                bine2 = Expression.Equal(left, right);
                                break;

                            case ExpressionType.NotEqual:
                                bine2 = Expression.NotEqual(left, right);
                                break;

                            case ExpressionType.LessThan:
                                bine2 = Expression.LessThan(left, right);
                                break;

                            case ExpressionType.LessThanOrEqual:
                                bine2 = Expression.LessThanOrEqual(left, right);
                                break;

                            case ExpressionType.GreaterThan:
                                bine2 = Expression.GreaterThan(left, right);
                                break;

                            case ExpressionType.GreaterThanOrEqual:
                                bine2 = Expression.GreaterThanOrEqual(left, right);
                                break;

                            default:
                                throw new NotImplementedException();
                        }

                        Func<ValueObjectComposite, ValueObjectComposite, bool> comparator = (Func<ValueObjectComposite, ValueObjectComposite, bool>)Expression.Lambda(bine2, px, py).Compile();
                        return new Condition(comparator, leftKeyPath, rightKeyPath);
                        // break;
                }

                throw new NotImplementedException();
            }

            private static Expression MakeUnpackComposite(Expression expr)
            {
                if (expr.Type != typeof(ValueObjectComposite))
                {
                    return expr;
                }

                Func<ValueObjectComposite, object?> f = x => x.Object;
                return Expression.Invoke(Expression.Constant(f), expr);
            }

            private static (Expression, string[]) MakeExpression(Expression exp, ParameterExpression pep)
            {
                switch (exp)
                {
                    case MemberExpression me:
                        var (expr, keypath) = MakeExpression(me.Expression, pep);
                        if (expr.Type == typeof(ValueObjectComposite))
                        {
                            Func<ValueObjectComposite, object?> memberfunc = (ValueObjectComposite voc) => voc.MemberGet(me.Member);
                            return (Expression.Convert(Expression.Invoke(Expression.Constant(memberfunc), expr), me.Member.GetMemberType()), keypath.Append(me.Member.Name.ToSnake()).ToArray());
                        }
                        else
                        {
                            return (Expression.MakeMemberAccess(expr, me.Member), keypath.Append(me.Member.Name.ToSnake()).ToArray());
                        }

                    case ParameterExpression pe:
                        return (pep, Array.Empty<string>());
                }

                throw new NotImplementedException();
            }
        }
    }
}
