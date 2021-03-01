using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class SqlStatement
    {
        public SqlElement Root { get; set; }

        public ParamInfo ParamInfo { get; set; } = new ParamInfo();

        public SqlStatement(SqlElement root)
        {
            Root = root;
        }
    }


    public abstract class SqlSelectStatement : SqlStatement
    {
        public abstract SqlParameterElement[] Parameters { get; }

        public abstract SqlJoinElement[] Relations { get; }

        public abstract SqlElement Conditions { get;  }

        public SqlSelectStatement(SqlElement root) : base(root) { }

        [Obsolete]
        public ColumnAliasingInfo ColumnAliasingInfo { get; } = new ColumnAliasingInfo();
    }

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left = null;
        public T1 Right = null;
    }

    public class SelectStatement<T> : SqlSelectStatement, IEnumerable<T> where T : class
    {
        public Func<SelectStatement<T>, IEnumerable<T>> OnExecute = null;

        public SelectStatement(SqlElement root) : base(root)
        {
            FinishBuild();
        }

        public virtual JoinedSelectStatement<Joined<T, T2>, T, T2> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> exp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            var lambdaBody = (exp as LambdaExpression).Body;
            var propinfo = ((lambdaBody as MemberExpression).Member as PropertyInfo);
            var hasmany = propinfo.GetCustomAttribute<HasManyAttribute>();

            return this.BuildJoins<T2>(propinfo, hasmany.ForeignKey, hasmany.PrimaryKey, cond);
        }

        protected virtual JoinedSelectStatement<Joined<T, T2>, T, T2> BuildJoins<T2>(MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T2, bool>> cond) where T2 : class
        {
            return new JoinedSelectStatement<Joined<T, T2>, T, T2>(this, memberInfo, foreignKey, primaryKey, cond) { OnExecute = this.OnExecute };
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return OnExecute(this).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return OnExecute(this).GetEnumerator();
        }

        public void FinishBuild()
        {
            var element = Root;
            var selectElement = PickupElement<SqlBinaryElement>(element, x => x is SqlBinaryElement && (x as SqlBinaryElement).Verb == SqlElementVerbs.Lambda, x => (x as SqlBinaryElement).Left);
            var parameter = selectElement.Right as SqlParameterElement;
            var parameters = new List<SqlParameterElement>() { };
            var conditions = selectElement.Left;

            parameter.ParamKey = ParamInfo.RegisterParameter(parameter.ParamKey, parameter.Type, null);

            var joinings = new List<SqlJoinElement>();
            var current = element;
            while (current != null)
            {
                current = PickupElement<SqlJoinElement>(current, x => x is SqlJoinElement, x => (x as SqlJoinElement)?.Left);
                if (current is SqlJoinElement picked)
                {
                    joinings.Add(picked);
                    parameters.Add(picked.Right as SqlParameterElement);
                    current = picked.Left;

                    var px = (picked.Right as SqlParameterElement);
                    px.ParamKey = ParamInfo.RegisterParameter(px.ParamKey, px.Type, picked.MemberInfo);
                }
            }
            joinings.Reverse();
            parameters.Reverse();
            parameters.Insert(0, parameter);

            foreach (var je in joinings)
            {
                var paramname = (je.Right as SqlParameterElement).ParamKey.ToLower();
                ParamInfo.SetBindingByKey(paramname, je.ForeignKey, je.PrimaryKey ?? parameter.Type.GetPrimaryKey());
            }

            _parameters = parameters.ToArray();
            _relations = joinings.ToArray();
            _conditions = conditions;
        }

        private SqlParameterElement[] _parameters = null;
        private SqlJoinElement[] _relations = null;
        private SqlElement _conditions = null;

        public override SqlParameterElement[] Parameters { get => _parameters; }
        public override SqlJoinElement[] Relations { get => _relations; }
        public override SqlElement Conditions { get => _conditions; }

        private T1 PickupElement<T1>(SqlElement element, Func<SqlElement, bool> matcher, Func<SqlElement, SqlElement> nextElement) where T1 : SqlElement
        {
            var current = element;

            while (current != null)
            {
                if (matcher(current))
                {
                    return current as T1;
                }

                current = nextElement(current);
            }

            return null;
        }
    }

    public class JoinedSelectStatement<JT, T, T1> : SelectStatement<T>
        where JT : Joined<T, T1>
        where T : class
        where T1 : class
    {
        // public string ForeignKey = null;
        // public string PrimaryKey = null;

        public JoinedSelectStatement(SelectStatement<T> statement, MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T1, bool>> conditions) : base(MakeSqlJoinElement(statement.Root, memberInfo, foreignKey, primaryKey, conditions))
        {
        }

        protected override JoinedSelectStatement<Joined<T, T2>, T, T2> BuildJoins<T2>(MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T2, bool>> cond) where T2 : class
        {
            return new JoinedSelectStatement<Joined<T, T2>, T, T2>(this, memberInfo, foreignKey, primaryKey, cond) { OnExecute = this.OnExecute };
        }

        private static SqlElement MakeSqlJoinElement(SqlElement root, MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T1, bool>> cond)
        {
            var propinfo = (memberInfo.MemberType == MemberTypes.Property) ? (memberInfo as PropertyInfo) : null;
            var t = propinfo.PropertyType;
            if (t.IsGenericType)
            {
                t = t.GetGenericArguments()[0];
            }
            return new SqlJoinElement(root, new SqlParameterElement(t, "t"), propinfo, foreignKey, primaryKey, cond);
        }
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

    public class SqlJoinElement : SqlBinaryElement
    {
        public string ForeignKey = null;
        public string PrimaryKey = null;
        public MemberInfo MemberInfo = null;
        public Expression Conditions = null;

        public SqlJoinElement(SqlElement left, SqlElement right, MemberInfo memberInfo, string foreignKey, string primaryKey, Expression cond) : base(SqlElementVerbs.Join, left, right)
        {
            ForeignKey = foreignKey;
            PrimaryKey = primaryKey;
            MemberInfo = memberInfo;
            Conditions = cond;
        }
    }

    public class SqlParameterElement : SqlElement
    {
        public Type Type;
        public string ParamKey { get; set; }

        public SqlParameterElement(Type type, string key)
        {
            Type = type;
            ParamKey = key;
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
