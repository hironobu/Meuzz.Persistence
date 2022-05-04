using System;
using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using MySql.Data.MySqlClient;

namespace Meuzz.Persistence.MySql
{
    public class DatabaseEngineProvider : IDatabaseEngineProvider
    {
        public void Register(DatabaseEngineFactory factory)
        {
            factory.Register("mysql", typeof(MySqlEngine));
        }
    }

    public class MySqlEngine : IDatabaseEngine
    {
        public void Configure(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void Configure(IDictionary<string, object> parameters)
        {
            Configure(new MySqlConnectionStringBuilder()
            {
                Server = parameters["host"].ToString(),
                Port = Convert.ToUInt32(parameters["port"]),
                Database = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString);
        }

        public IDatabaseContext CreateContext()
        {
            var connection = new MySqlConnection(_connectionString);

            return new DatabaseContextBase(connection, _formatter);
        }

        private string _connectionString;

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
