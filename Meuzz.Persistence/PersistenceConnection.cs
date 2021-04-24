using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Meuzz.Persistence.Sql;
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

    public interface IPersistenceEngineProvider
    {
        void Register(PersistenceEngineFactory factory);
    }

    public interface IPersistenceEngine
    {
        // Connection CreateConnection(IDictionary<string, object> parameters);

        // SqlFormatter CreateFormatter();

        IPersistenceContext CreateContext(IDictionary<string, object> parameters);
    }

    public interface IPersistenceContext
    {
        PersistenceConnection Connection { get; }
        SqlFormatter Formatter { get; }
    }

    public class PersistenceContextBase : IPersistenceContext
    {
        public PersistenceConnection Connection { get; }
        public SqlFormatter Formatter { get; }

        public PersistenceContextBase(PersistenceConnection connection, SqlFormatter formatter)
        {
            Connection = connection;
            Formatter = formatter;
        }
    }

    public abstract class PersistenceConnection : IDisposable
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

    public abstract class DbConnectionImpl<T, T1> : PersistenceConnection
        where T : DbConnection
        where T1 : DbCommand
    {
        private T _connection;

        protected void SetupConnection(T conn)
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
}
