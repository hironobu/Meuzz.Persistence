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
        public Connection CreateConnection(IDictionary<string, object> parameters)
        {
            return new SqliteConnectionImpl(parameters);
        }

        public SqlFormatter CreateFormatter()
        {
            return new SqliteFormatter();
        }
    }

    public class SqliteConnectionImpl : DbConnectionImpl<SqliteConnection, SqliteCommand>
    {
        public SqliteConnectionImpl(IDictionary<string, object> parameters)
        {
            SetupConnection(new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = parameters["file"].ToString() }.ToString()));
        }

        protected override void RegisterParameter(SqliteCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }

    public class SqliteFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT (last_insert_rowid() - {rows - 1}) AS {pkey};";
        }
    }
}
