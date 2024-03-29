﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Meuzz.Foundation;
using Meuzz.Persistence.Sql;

namespace Meuzz.Persistence.Database
{
    public interface IDatabaseEngineProvider
    {
        void Register(DatabaseEngineFactory factory);
    }

    public interface IDatabaseEngine
    {
        void Configure(string connectionString);

        void Configure(IDictionary<string, object> parameters);

        IDatabaseContext CreateContext();
    }

    public interface IDatabaseContext : IDisposable
    {
        void Open();

        ResultSet? Execute(SqlStatement statement);

        ResultSet? Execute(string sql, IDictionary<string, object?>? parameters = null);

        void Close();
    }

    public class DatabaseContextBase : IDatabaseContext
    {
        public DatabaseContextBase(DbConnection connection, SqlFormatter formatter)
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
            var (sql, parameters, columnCollationInfo) = Formatter.Format(statement);
            if (sql == null)
            {
                return null;
            }

            var rset = Execute(sql, parameters);

            return columnCollationInfo != null ? new SqlCollator().Collate(rset, columnCollationInfo) : rset;
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
        public ResultSet(IEnumerable<IDictionary<string, object?>> results)
        {
            Results = results;
        }

        public ResultSet(DbDataReader reader)
        {
            var results = new List<IDictionary<string, object?>>();

            while (reader.HasRows)
            {
                var cols = Enumerable.Range(0, reader.FieldCount).Select(x => reader.GetName(x)).ToArray();
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

        public IEnumerable<IDictionary<string, object?>> Results { get; }

        public IEnumerable<IDictionary<string, object?>> Grouped()
        {
            return Results.Select(x =>
            {
                var kvs = x.Select(c => (c.Key.Split('.', 2), c.Value));
                var keyedDict = new Dictionary<string, object?>();

                foreach (var (kk, v) in kvs)
                {
                    var d = (Dictionary<string, object?>)keyedDict.GetValueOrFunc(kk[0], () => new Dictionary<string, object?>())!; // TODO: to be reviewed

                    d[kk[1].ToLower()] = v;
                }

                return keyedDict;
            });
        }
    }
}
