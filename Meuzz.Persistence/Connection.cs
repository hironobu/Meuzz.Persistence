using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;
using Meuzz.Persistence.Sql;

namespace Meuzz.Persistence
{

    public abstract class SqlConnectionContext
    {

    }

    public class SqliteConnectionContext : SqlConnectionContext
    {
        public ColumnAliasingInfo ColumnAliasingInfo { get; } = new ColumnAliasingInfo();
    }


    public class ConnectionFactory
    {
        public Connection NewConnection(string connectionString)
        {
            var parameters = ParseConnectionString(connectionString);

            switch (parameters["type"])
            {
                case "sqlite":
                    return new SqliteConnectionImpl(parameters["file"]);

                case "mssql":
                    return new MssqlConnectionImpl(parameters["host"], Int32.Parse(parameters["port"]), parameters["database"], parameters["user"], parameters["password"]);
            }

            throw new NotImplementedException();
        }

        private IDictionary<string, string> ParseConnectionString(string connectionString)
        {
            return connectionString.Split(";").Select(x => x.Split("=", 2)).ToDictionary(x => x[0], x => x[1]);
        }
    }

    public abstract class Connection : IDisposable
    {
        private bool _disposed;

        // ~Connection()
        // {
        //     Dispose(disposing: false);
        // }

        public abstract void Open();

        public virtual ResultSet Execute(string sql, SqlConnectionContext context = null) { return Execute(sql, null, context); }

        public abstract ResultSet Execute(string sql, IDictionary<string, object> parameters, SqlConnectionContext context = null);

        public abstract void Close();

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // for managed resources
                    Close();
                }

                // here for unmanaged resources
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public class ResultSet
        {
            public IEnumerable<IDictionary<string, object>> Results { get; set; }
        }
    }

    public class SqliteConnectionImpl : Connection
    {
        private SqliteConnection _connection;

        public SqliteConnectionImpl(string path)
        {
            var sqlConnectionSb = new SqliteConnectionStringBuilder { DataSource = path };
            _connection = new SqliteConnection(sqlConnectionSb.ToString());
        }

        public override void Open()
        {
            _connection.Open();
        }

        public override ResultSet Execute(string sql, IDictionary<string, object> parameters, SqlConnectionContext context)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql.ToString();
            if (parameters != null)
            {
                foreach (var (k, v) in parameters)
                {
                    cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
                }
            }
            using var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
            return new SqliteResultSet(reader);
        }

        public override void Close()
        {
            _connection.Close();
        }

        class SqliteResultSet : ResultSet
        {
            public SqliteResultSet(/*SqliteSelectStatement statement, */ SqliteDataReader reader)
            {
                var results = new List<IDictionary<string, object>>();

                while (reader.HasRows)
                {
                    // var table = reader.GetSchemaTable().Rows[0]["BaseTableName"];
                    // var t = statement.GetTableType();
                    var cols = Enumerable.Range(0, reader.FieldCount).Select(x => reader.GetName(x)).ToArray<string>();
                    while (reader.Read())
                    {
                        var vals = Enumerable.Range(0, reader.FieldCount).Select(x => reader.IsDBNull(x) ? null : reader.GetValue(x)).ToArray();
                        var dict = cols.Zip(vals, (k, v) => new { K = k, V = v }).ToDictionary(x => x.K.ToLower(), x => x.V);

                        // var entity = PopulateEntity(t, cols, vals);

                        results.Add(dict);
                    }
                    reader.NextResult();
                }

                Results = results;
            }
        }
    }

    public class MssqlConnectionImpl : Connection
    {
        private SqlConnection _connection;

        public MssqlConnectionImpl(string host, int port, string databaseName, string user, string password)
        {
            var builder = new SqlConnectionStringBuilder();
            builder.DataSource = $"{host},{port}";
            builder.InitialCatalog = databaseName;
            builder.UserID = user;
            builder.Password = password;

            _connection = new SqlConnection(builder.ConnectionString);
        }

        public override void Close()
        {
            _connection.Close();
        }

        public override ResultSet Execute(string sql, IDictionary<string, object> parameters, SqlConnectionContext context = null)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql.ToString();
            if (parameters != null)
            {
                foreach (var (k, v) in parameters)
                {
                    cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
                }
            }
            using var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
            return new MssqlResultSet(reader);
        }

        public override void Open()
        {
            _connection.Open();
        }

        class MssqlResultSet : ResultSet
        {
            public MssqlResultSet(SqlDataReader reader)
            {
                var results = new List<IDictionary<string, object>>();

                while (reader.HasRows)
                {
                    // var table = reader.GetSchemaTable().Rows[0]["BaseTableName"];
                    // var t = statement.GetTableType();
                    var cols = Enumerable.Range(0, reader.FieldCount).Select(x => reader.GetName(x)).ToArray<string>();
                    while (reader.Read())
                    {
                        var vals = Enumerable.Range(0, reader.FieldCount).Select(x => reader.IsDBNull(x) ? null : reader.GetValue(x)).ToArray();
                        var dict = cols.Zip(vals, (k, v) => new { K = k, V = v }).ToDictionary(x => x.K.ToLower(), x => x.V);

                        // var entity = PopulateEntity(t, cols, vals);

                        results.Add(dict);
                    }
                    reader.NextResult();
                }

                Results = results;
            }
        }
    }
}
