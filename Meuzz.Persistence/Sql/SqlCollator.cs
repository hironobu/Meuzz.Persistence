﻿#nullable enable

using System.Linq;
using Meuzz.Persistence.Database;

namespace Meuzz.Persistence.Sql
{
    public class SqlCollator
    {
        public ResultSet Collate(ResultSet rset, ColumnCollationInfo columnCollationInfo)
        {
            return new ResultSet(rset.Results.Select(r => r.ToDictionary(x => columnCollationInfo.GetOutputColumnName(x.Key), x => x.Value)));
        }
    }
}
