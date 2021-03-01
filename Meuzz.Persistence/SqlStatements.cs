using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class SqlStatement
    {
        public SqlElement Root { get; set; }
        public TypeInfo TypeInfo { get; set; }

        public SqlStatement(SqlElement root, TypeInfo typeInfo)
        {
            Root = root;
            TypeInfo = typeInfo.ForStatement();
        }
    }

/*    public class SqlSelectStatement : SqlStatement
    {
        public SqlSelectStatement(SqlElement root) : base(root) { }
        // public SqlElement Root { get; set; }
    }*/

    public class Joined<T0, T1>
        where T0 : class
        where T1 : class
    {
        public T0 Left = null;
        public T1 Right = null;
    }

    public class SelectStatement<T> : SqlStatement where T : class
    {
        public Func<SelectStatement<T>, IEnumerable<T>> OnExecute = null;

        public SelectStatement(SqlElement root, TypeInfo typeInfo) : base(root, typeInfo) {}

        public virtual JoinedSelectStatement<Joined<T, T2>, T, T2> Joins<T2>(Expression<Func<T, IEnumerable<T2>>> exp, Expression<Func<T, T2, bool>> cond = null) where T2 : class, new()
        {
            var lambdaBody = (exp as LambdaExpression).Body;
            var propinfo = ((lambdaBody as MemberExpression).Member as PropertyInfo);
            var hasmany = propinfo.GetCustomAttribute<HasManyAttribute>();

            return this.BuildJoins<T2>(propinfo, hasmany.ForeignKey, hasmany.PrimaryKey, cond);
        }

        protected virtual JoinedSelectStatement<Joined<T, T2>, T, T2> BuildJoins<T2>(MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T2, bool>> cond) where T2 : class
        {
            return new JoinedSelectStatement<Joined<T, T2>, T, T2>(this, memberInfo, foreignKey, primaryKey, cond, this.TypeInfo) { OnExecute = this.OnExecute };
        }

        public IEnumerable<T> Execute()
        {
            return OnExecute(this);
        }
    }

    public class JoinedSelectStatement<JT, T, T1> : SelectStatement<T>
        where JT : Joined<T, T1>
        where T : class
        where T1 : class
    {
        // public string ForeignKey = null;
        // public string PrimaryKey = null;

        public JoinedSelectStatement(SelectStatement<T> statement, MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T1, bool>> conditions, TypeInfo typeInfo) : base(MakeSqlJoinElement(statement.Root, memberInfo, foreignKey, primaryKey, conditions), typeInfo)
        {
        }

        protected override JoinedSelectStatement<Joined<T, T2>, T, T2> BuildJoins<T2>(MemberInfo memberInfo, string foreignKey, string primaryKey, Expression<Func<T, T2, bool>> cond) where T2 : class
        {
            return new JoinedSelectStatement<Joined<T, T2>, T, T2>(this, memberInfo, foreignKey, primaryKey, cond, this.TypeInfo) { OnExecute = this.OnExecute };
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
