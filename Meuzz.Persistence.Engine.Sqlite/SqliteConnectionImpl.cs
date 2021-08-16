using System;
using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using Microsoft.Data.Sqlite;

namespace Meuzz.Persistence.Sqlite
{
    public class PersistenceEngineProvider : IPersistenceEngineProvider
    {
        public void Register(PersistenceEngineFactory factory)
        {
            factory.Register("sqlite", new SqliteEngine());
        }
    }

    public class SqliteEngine : IPersistenceEngine
    {
        public IStorageContext CreateContext(IDictionary<string, object> parameters)
        {
            var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = parameters["file"].ToString() }.ToString());

            return new StorageContextBase(connection, _formatter);
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
