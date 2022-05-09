using System;
using System.Collections.Generic;
using Meuzz.Persistence.Database;
using Meuzz.Persistence.Sql;
using Microsoft.Data.SqlClient;

namespace Meuzz.Persistence.Engine.Mssql
{
    public class DatabaseEngineProvider : IDatabaseEngineProvider
    {
        public void Register(DatabaseEngineFactory factory)
        {
            factory.Register("mssql", typeof(MssqlEngine));
        }
    }

    public class MssqlEngine : IDatabaseEngine
    {
        public void Configure(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void Configure(IDictionary<string, object> parameters)
        {
            Configure(new SqlConnectionStringBuilder()
            {
                DataSource = $"{parameters["host"]},{parameters["port"]}",
                InitialCatalog = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString);
        }

        public IDatabaseContext CreateContext()
        {
            var connection = new SqlConnection(_connectionString);

            return new DatabaseContextBase(connection, _formatter);
        }

        private string _connectionString;

        private SqlFormatter _formatter = new MssqlFormatter();
    }

    public class MssqlFormatter : SqlFormatter
    {
        protected override string GetInsertIntoOutputString() => $"OUTPUT INSERTED.ID";
    }
}
