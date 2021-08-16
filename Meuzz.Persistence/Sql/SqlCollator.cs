#nullable enable

using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class SqlCollator
    {
        public SqlCollator(ColumnCollationInfo columnCollationInfo)
        {
            _columnCollationInfo = columnCollationInfo;
        }

        public ResultSet Collate(ResultSet rset)
        {
            return new ResultSet(rset.Results.Select(r => r.ToDictionary(x => _columnCollationInfo.GetOriginalColumnName(x.Key), x => x.Value)));
        }

        private ColumnCollationInfo _columnCollationInfo;
    }
}
