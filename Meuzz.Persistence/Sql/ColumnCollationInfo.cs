#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class ColumnCollationInfo
    {
        public ColumnCollationInfo()
        {
        }

        public IDictionary<string, string> MakeColumnAliasingDictionary(string paramName, IEnumerable<string> columns)
        {
            return MakeColumnAliasingDictionary(paramName, columns.Select(c => new[] { c }));
        }

        public IDictionary<string, string> MakeColumnAliasingDictionary(string paramName, IEnumerable<string[]> columns)
        {
            var columnKeys = columns.Select(x => $"{paramName}.{x.First()}");

            foreach (var ks in columns)
            {
                var ck = $"_c{_columns.Count()}";

                if (!_columns.ContainsKey(ks.First()))
                {
                    _columns.Add($"{paramName}.{ks.First()}", ck);
                    _outputColumns.Add(ck, $"{paramName}.{ks.Last()}");
                }
            }

            return _columns.Where(x => columnKeys.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value);
        }

        public string _GetOriginalColumnName(string c)
        {
            return _columns.First(p => p.Value == c).Key;
        }

        public string GetOutputColumnName(string c)
        {
            return _outputColumns[c];
        }

        public string[] GetAliases()
        {
            return _outputColumns.Keys.ToArray();
        }

        private IDictionary<string, string> _columns = new Dictionary<string, string>();
        private IDictionary<string, string> _outputColumns = new Dictionary<string, string>();
        //private IDictionary<string, string> _aliasingColumns = new Dictionary<string, string>();
        //private IDictionary<string, string> _aliasingOutputColumns = new Dictionary<string, string>();
    }
}
