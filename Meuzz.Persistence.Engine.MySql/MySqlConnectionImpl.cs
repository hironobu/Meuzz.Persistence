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
        public IStorageContext CreateContext(IDictionary<string, object> parameters)
        {
            var connection = new MySqlConnection(new MySqlConnectionStringBuilder()
            {
                Server = parameters["host"].ToString(),
                Port = Convert.ToUInt32(parameters["port"]),
                Database = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString);

            return new StorageContextBase(connection, _formatter);
        }

        private SqlFormatter _formatter = new MySqlFormatter();
    }

    public class MySqlFormatter : SqlFormatter
    {
        protected override string GetLastInsertedIdString(string pkey, int rows)
        {
            return $"SELECT last_insert_id() AS {pkey};";
        }
    }
}
