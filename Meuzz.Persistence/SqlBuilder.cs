using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Meuzz.Persistence
{
    public abstract class SqlBuilderBase
    {
    }

    public abstract class SqlBuilder<T> : SqlBuilderBase where T : class
    {
        public abstract SelectStatement<T> BuildSelect();
    }

    public class SqliteSqlBuilder<T> : SqlBuilder<T> where T : class
    {
        public override SelectStatement<T> BuildSelect()
        {
            return new SelectStatement<T>();
        }
    }
}
