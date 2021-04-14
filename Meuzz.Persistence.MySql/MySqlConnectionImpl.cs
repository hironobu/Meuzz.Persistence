using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace Meuzz.Persistence.MySql
{
    public class PersistenceServiceProvider : IPersistenceServiceProvider
    {
        public void Register(ConnectionFactory connectionFactory)
        {
            connectionFactory.RegisterConnectionType("mysql", typeof(MySqlConnectionImpl));
        }
    }

    public class MySqlConnectionImpl : DbConnectionImpl<MySqlConnection, MySqlCommand>
    {
        public MySqlConnectionImpl(IDictionary<string, object> parameters)
        {
            SetupConnection(new MySqlConnection(new MySqlConnectionStringBuilder()
            {
                Server = parameters["host"].ToString(),
                Port = Convert.ToUInt32(parameters["port"]),
                Database = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString));
        }

        protected override void RegisterParameter(MySqlCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }
}
