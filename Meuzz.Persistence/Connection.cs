using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;
using Meuzz.Persistence.Sql;
using MySql.Data.MySqlClient;
using System.Data.Common;

namespace Meuzz.Persistence
{

    public class SqlConnectionContext
    {
        public ColumnCollationInfo ColumnCollationInfo { get; set; } = null;
    }

    /*public class SqliteConnectionContext : SqlConnectionContext
    {
        public ColumnAliasingInfo ColumnAliasingInfo { get; set; } = new ColumnAliasingInfo();
    }*/


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

                case "mysql":
                    return new MySqlConnectionImpl(parameters["host"], Int32.Parse(parameters["port"]), parameters["database"], parameters["user"], parameters["password"]);
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

            public ResultSet(DbDataReader reader)
            {
                var results = new List<IDictionary<string, object>>();

                while (reader.HasRows)
                {
                    var cols = Enumerable.Range(0, reader.FieldCount).Select(x => reader.GetName(x)).ToArray<string>();
                    while (reader.Read())
                    {
                        var vals = Enumerable.Range(0, reader.FieldCount).Select(x => reader.IsDBNull(x) ? null : reader.GetValue(x)).ToArray();
                        var dict = cols.Zip(vals, (k, v) => new { K = k, V = v }).ToDictionary(x => x.K.ToLower(), x => x.V);

                        results.Add(dict);
                    }
                    reader.NextResult();
                }

                Results = results;
            }
        }
    }

    public abstract class DbConnectionImpl<T, T1> : Connection
        where T : DbConnection
        where T1 : DbCommand
    {
        private T _connection;

        public DbConnectionImpl(T conn)
        {
            _connection = conn;
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
                    RegisterParameter(cmd as T1, k, v != null ? v : DBNull.Value);
                }
            }
            using var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
            return new ResultSet(reader);
        }

        protected abstract void RegisterParameter(T1 cmd, string k, object v);

        public override void Close()
        {
            _connection.Close();
        }
    }

    public class SqliteConnectionImpl : DbConnectionImpl<SqliteConnection, SqliteCommand>
    {
        public SqliteConnectionImpl(string path)
            : base(new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path }.ToString()))
        {
        }

        protected override void RegisterParameter(SqliteCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }

    public class MssqlConnectionImpl : DbConnectionImpl<SqlConnection, SqlCommand>
    {
        public MssqlConnectionImpl(string host, int port, string databaseName, string user, string password)
            : base(new SqlConnection(new SqlConnectionStringBuilder()
            {
                DataSource = $"{host},{port}",
                InitialCatalog = databaseName,
                UserID = user,
                Password = password
            }.ConnectionString))
        {
        }

        protected override void RegisterParameter(SqlCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }

    public class MySqlConnectionImpl : DbConnectionImpl<MySqlConnection, MySqlCommand>
    {
        public MySqlConnectionImpl(string host, int port, string databaseName, string user, string password)
            : base(new MySqlConnection(new MySqlConnectionStringBuilder()
            {
                Server = host,
                Port = (uint)port,
                Database = databaseName,
                UserID = user,
                Password = password 
            }.ConnectionString))
        {
        }

        protected override void RegisterParameter(MySqlCommand cmd, string k, object v)
        {
            cmd.Parameters.AddWithValue(k, v != null ? v : DBNull.Value);
        }
    }
}
