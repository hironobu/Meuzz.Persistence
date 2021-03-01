using System.Collections.Generic;
using System.Linq;

namespace Meuzz.Persistence
{
    public abstract class SqlCollator
    {
        public abstract IDictionary<string, object> Collate(IDictionary<string, object> x, SqlConnectionContext context);
    }

    public class SqliteCollator : SqlCollator
    {
        public override IDictionary<string, object> Collate(IDictionary<string, object> x, SqlConnectionContext context)
        {
            return x.ToDictionary(x => (context as SqliteConnectionContext).ColumnAliasingInfo.GetOriginalColumnName(x.Key), x => x.Value);
        }
    }

}
