#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Meuzz.Persistence.Sql
{
    public class ParameterSetInfo
    {
        public ParameterSetInfo()
        {
            _parameters = new Dictionary<string, Type>();
            _defaultParamName = null;
        }

        public ParameterSetInfo(ParameterSetInfo parameterSetInfo)
        {
            _parameters = new Dictionary<string, Type>(parameterSetInfo._parameters);
            _defaultParamName = parameterSetInfo._defaultParamName;
        }

        public void ResetParameters()
        {
            _parameters.Clear();
        }

        public string RegisterParameter(string? name, Type t, bool asDefault)
        {
            string k;

            if (!asDefault)
            {
                int i = 1;

                string k0 = "_t";
                k = $"{k0}{i++}";

                while (_parameters.ContainsKey(k))
                {
                    k = $"{k0}{i++}";
                }
                _parameters.Add(k, t);
            }
            else
            {
                k = $"_t0";

                if (_parameters.TryGetValue(k, out var t0) && t0 != t)
                {
                    throw new InvalidOperationException();
                }

                _parameters[k] = t;
                _defaultParamName = k;
            }

            return k;
        }

        public Type? GetTypeByName_(string name)
        {
            //return _defaultParamName != name ? _parameters[name] : null;
            return _parameters.TryGetValue(name, out var value) ? value : null;
        }

        public string GetName(Expression expr)
        {
            foreach (var x in _parameters)
            {
                if (!(expr is ParameterExpression pe))
                {
                    throw new NotImplementedException();
                }

                if (x.Value == pe.Type)
                {
                    return x.Key;
                }
            }

            throw new InvalidOperationException();
        }

        public IEnumerable<(string, Type)> GetAllParameters()
        {
            return _parameters.Select(x => (x.Key, x.Value));
        }

        public string GetDefaultParamName()
        {
            if (_defaultParamName == null)
            {
                throw new NotImplementedException();
            }

            return _defaultParamName;
        }

        private IDictionary<string, Type> _parameters;
        private string? _defaultParamName;
    }
}
