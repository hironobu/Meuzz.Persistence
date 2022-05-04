using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using Microsoft.Data.Sqlite;

namespace Meuzz.Persistence.Sqlite
{
    public class DatabaseEngineProvider : IDatabaseEngineProvider
    {
        public void Register(DatabaseEngineFactory factory)
        {
            factory.Register("sqlite", new SqliteEngine());
        }
    }

    public class SqliteEngine : IDatabaseEngine
    {
        public IDatabaseContext CreateContext(IDictionary<string, object> parameters)
        {
            var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = parameters["file"].ToString() }.ToString());

            return new DatabaseContextBase(connection, _formatter);
        }

        private SqlFormatter _formatter = new SqliteFormatter();
    }

    public class SqliteFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT (last_insert_rowid() - {rows - 1}) AS {pkey};";
        }
    }
}
