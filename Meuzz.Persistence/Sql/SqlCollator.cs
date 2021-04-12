using System.Collections.Generic;
using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class SqlCollator
    {
        public IDictionary<string, object> Collate(IDictionary<string, object> x, SqlConnectionContext context)
        {
            return x.ToDictionary(x => context.ColumnAliasingInfo.GetOriginalColumnName(x.Key), x => x.Value);
        }
    }
}
