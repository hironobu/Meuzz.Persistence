#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Meuzz.Persistence.Sql;
using System.Data.Common;

namespace Meuzz.Persistence
{
    public interface IPersistenceEngineProvider
    {
        void Register(PersistenceEngineFactory factory);
    }

    public interface IPersistenceEngine
    {
        IStorageContext CreateContext(IDictionary<string, object> parameters);
    }

    public interface IStorageContext
    {
        void Open();

        ResultSet? Execute(SqlStatement statement);

        ResultSet? Execute(string sql, IDictionary<string, object?>? parameters = null);

        void Close();
    }

    public class StorageContextBase : IStorageContext
    {
        public StorageContextBase(DbConnection connection, SqlFormatter formatter)
        {
            Connection = connection;
            Formatter = formatter;
        }

        // ~Connection()
        // {
        //     Dispose(disposing: false);
        // }

        public DbConnection Connection { get; }

        public SqlFormatter Formatter { get; }

        public virtual void Open()
        {
            Connection.Open();
        }

        public virtual ResultSet? Execute(SqlStatement statement)
        {
            var (sql, parameters, collator) = Formatter.Format(statement);
            if (sql == null)
            {
                return null;
            }

            var rset = Execute(sql, parameters);

            return collator != null ? collator.Collate(rset) : rset;
        }

        public virtual ResultSet Execute(string sql, IDictionary<string, object?>? parameters = null)
        {
            using var cmd = Connection.CreateCommand();
            cmd.CommandText = sql.ToString();
            if (parameters != null)
            {
                foreach (var (k, v) in parameters)
                {
                    RegisterParameter(cmd, k, v);
                }
            }
            using var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
            return new ResultSet(reader);
        }

        protected void RegisterParameter(DbCommand cmd, string k, object? v)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = k;
            p.Value = v ?? DBNull.Value;

            cmd.Parameters.Add(p);
        }

        public virtual void Close()
        {
            Connection.Close();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // for managed resources
                    Close();

                    Connection.Dispose();
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

        private bool _disposed;
    }

    public class ResultSet
    {
        public IEnumerable<IDictionary<string, object?>> Results { get; }

        public ResultSet(IEnumerable<IDictionary<string, object?>> results)
        {
            Results = results;
        }

        public ResultSet(DbDataReader reader)
        {
            var results = new List<IDictionary<string, object?>>();

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
