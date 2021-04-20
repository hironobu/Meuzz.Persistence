using System.Collections.Generic;
using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class SqlCollator
    {
        public PersistenceConnection.ResultSet Collate(PersistenceConnection.ResultSet rset, ColumnCollationInfo columnCollationInfo)
        {
            return new PersistenceConnection.ResultSet(rset.Results.Select(r => r.ToDictionary(x => columnCollationInfo.GetOriginalColumnName(x.Key), x => x.Value)));
        }
    }
}
