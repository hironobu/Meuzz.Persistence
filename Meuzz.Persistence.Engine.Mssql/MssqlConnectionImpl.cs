using System;
using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using Microsoft.Data.SqlClient;

namespace Meuzz.Persistence.Engine.Mssql
{
    public class PersistenceEngineProvider : IPersistenceEngineProvider
    {
        public void Register(PersistenceEngineFactory factory)
        {
            factory.Register("mssql", new MssqlEngine());
        }
    }

    public class MssqlEngine : IPersistenceEngine
    {
        public Connection CreateConnection(IDictionary<string, object> parameters)
        {
            return new MssqlConnectionImpl(parameters);
        }

        public SqlFormatter CreateFormatter()
        {
            return new MssqlFormatter();
        }
    }

    public class MssqlConnectionImpl : DbConnectionImpl<SqlConnection, SqlCommand>
    {
        public MssqlConnectionImpl(IDictionary<string, object> parameters)
        {
            SetupConnection(new SqlConnection(new SqlConnectionStringBuilder()
            {
                DataSource = $"{parameters["host"]},{parameters["port"]}",
                InitialCatalog = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString));
        }

        protected override void RegisterParameter(SqlCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }

    public class MssqlFormatter : SqlFormatter
    {
        protected override string GetInsertIntoOutputString() => $"OUTPUT INSERTED.ID";
    }
}
