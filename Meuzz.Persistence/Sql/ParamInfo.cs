#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;

namespace Meuzz.Persistence.Sql
{
    public class ParamInfo
    {
        private IDictionary<string, Type> _parameters = new Dictionary<string, Type>();
        private string? _defaultParamName = null;
        private Type? _defaultParamType = null;

        public void ResetParameters()
        {
            _parameters.Clear();
        }

        public string? RegisterParameter(string? name, Type t, bool asDefault)
        {
            var k = name;

            if (k != null)
            {
                int i = 1;
                while (_parameters.ContainsKey(k))
                {
                    k = $"{name}{i++}";
                }
                _parameters.Add(k, t);
            }

            if (asDefault)
            {
                _defaultParamType = t ?? _defaultParamType;
                _defaultParamName = name ?? _defaultParamName;
            }

            return k;
        }

        public Type GetParameterTypeByParamName(string name)
        {
            return _parameters[name];
        }

        public (string, Type)[] GetAllParameters()
        {
            return _parameters.Select(x => (x.Key, x.Value)).ToArray();
        }

        public Type? GetDefaultParamType()
        {
            return _defaultParamType;
        }

        public string? GetDefaultParamName()
        {
            return _defaultParamName;
        }
    }
}
