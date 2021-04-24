using System;
using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using MySql.Data.MySqlClient;

namespace Meuzz.Persistence.MySql
{
    public class PersistenceEngineProvider : IPersistenceEngineProvider
    {
        public void Register(PersistenceEngineFactory factory)
        {
            factory.Register("mysql", new MySqlEngine());
        }
    }

    public class MySqlEngine : IPersistenceEngine
    {
        /*public IPersistenceContext_ CreateConnection(IDictionary<string, object> parameters)
        {
            return new MySqlConnectionImpl(parameters);
        }

        public SqlFormatter CreateFormatter()
        {
            return new MySqlFormatter();
        }*/

        public IPersistenceContext CreateContext(IDictionary<string, object> parameters)
        {
            return new PersistenceContextBase(new MySqlConnectionImpl(parameters), new MySqlFormatter());
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

    public class MySqlFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT last_insert_id() AS {pkey};";
        }
    }
}
