﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace Meuzz.Persistence
{

    public abstract class SqlConnectionContext
    {

    }

    public class SqliteConnectionContext : SqlConnectionContext
    {
        public ColumnAliasingInfo ColumnAliasingInfo { get; } = new ColumnAliasingInfo();
    }

    public abstract class Connection : IDisposable
    {
        private bool _disposed = false;

        ~Connection()
        {
            Dispose();
        }

        public abstract void Open();

        public abstract ResultSet Execute(string sql, SqlConnectionContext context = null);

        public abstract void Close();

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    Close();
                }
            }
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
            var sqlConnectionSb = new SqliteConnectionStringBuilder { DataSource = ":memory:" };
            _connection = new SqliteConnection(sqlConnectionSb.ToString());
        }

        public override void Open()
        {
            _connection.Open();
        }

        public override ResultSet Execute(string sql, SqlConnectionContext context)
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = sql.ToString();
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
                    // var schema = reader.GetSchemaTable();
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
