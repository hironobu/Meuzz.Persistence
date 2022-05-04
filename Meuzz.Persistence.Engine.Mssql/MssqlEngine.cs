using System;
using System.Collections.Generic;
using Meuzz.Persistence.Sql;
using Microsoft.Data.SqlClient;

namespace Meuzz.Persistence.Engine.Mssql
{
    public class DatabaseEngineProvider : IDatabaseEngineProvider
    {
        public void Register(DatabaseEngineFactory factory)
        {
            factory.Register("mssql", new MssqlEngine());
        }
    }

    public class MssqlEngine : IDatabaseEngine
    {
        public IDatabaseContext CreateContext(IDictionary<string, object> parameters)
        {
            var connection = new SqlConnection(new SqlConnectionStringBuilder()
            {
                DataSource = $"{parameters["host"]},{parameters["port"]}",
                InitialCatalog = parameters["database"].ToString(),
                UserID = parameters["user"].ToString(),
                Password = parameters["password"].ToString()
            }.ConnectionString);

            return new DatabaseContextBase(connection, _formatter);
        }

        private SqlFormatter _formatter = new MssqlFormatter();
    }

    public class MssqlFormatter : SqlFormatter
    {
        protected override string GetInsertIntoOutputString() => $"OUTPUT INSERTED.ID";
    }
}
