using System.Collections.Generic;
using Meuzz.Persistence.Database;
using Meuzz.Persistence.Sql;
using Microsoft.Data.Sqlite;

namespace Meuzz.Persistence.Sqlite
{
    public class DatabaseEngineProvider : IDatabaseEngineProvider
    {
        public void Register(DatabaseEngineFactory factory)
        {
            factory.Register("sqlite", typeof(SqliteEngine));
        }
    }

    public class SqliteEngine : IDatabaseEngine
    {
        public void Configure(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void Configure(IDictionary<string, object> parameters)
        {
            _connectionString = new SqliteConnectionStringBuilder { DataSource = parameters["file"].ToString() }.ToString();
        }

        public IDatabaseContext CreateContext()
        {
            var connection = new SqliteConnection(_connectionString);

            return new DatabaseContextBase(connection, _formatter);
        }

        private string _connectionString;

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
