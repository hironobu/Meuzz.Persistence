using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace Meuzz.Persistence.Mssql
{
    public class PersistenceServiceProvider : IPersistenceServiceProvider
    {
        public void Register(ConnectionFactory connectionFactory)
        {
            connectionFactory.RegisterConnectionType("mssql", typeof(MssqlConnectionImpl));
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
}
