using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace Meuzz.Persistence.Sqlite
{
    public class PersistenceServiceProvider : IPersistenceServiceProvider
    {
        public void Register(ConnectionFactory connectionFactory)
        {
            connectionFactory.RegisterConnectionType("sqlite", typeof(SqliteConnectionImpl));
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
}
