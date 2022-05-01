#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class SqlValueStatement : SqlStatement
    {
        public string? PrimaryKey { get; private set; }
        public string[] Columns { get; private set; }

        public object[] Values { get => _values.ToArray(); }

        public IDictionary<string, object?>? ExtraData { get; set; } = null;

        public bool IsInsert { get; private set; }

        private List<object> _values = new List<object>();

        public SqlValueStatement(Type t, bool isInsert) : base(t)
        {
            var ci = t.GetClassInfo();
            if (ci == null) { throw new NotImplementedException(); }
            PrimaryKey = t.GetPrimaryKey();
            Columns = ci.Columns.Select(x => x.Name).Where(x => x != t.GetPrimaryKey()).ToArray();
            IsInsert = isInsert;
        }

        public virtual void Append<T>(IEnumerable<T> objs)
        {
            _values.AddRange(Enumerable.Cast<object>(objs));
        }
    }

    public class SqlInsertStatement : SqlValueStatement 
    {
        public SqlInsertStatement(Type t) : base(t, true)
        {
        }
    }

    public class SqlUpdateStatement : SqlValueStatement
    {
        public SqlUpdateStatement(Type t) : base(t, false)
        {
        }
    }

    public class InsertStatement<T> : SqlInsertStatement where T : class
    {
        public InsertStatement() : base(typeof(T))
        {
        }
    }

    public class UpdateStatement<T> : SqlUpdateStatement where T : class
    {
        public UpdateStatement() : base(typeof(T))
        {
        }
    }
}
