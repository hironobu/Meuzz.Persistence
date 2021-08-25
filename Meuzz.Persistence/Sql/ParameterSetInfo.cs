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
            _parameterMemberExpressions = new Dictionary<ExpressionComparer, MemberExpression[]>();
            _parameters = new Dictionary<string, Type>();
            _parameterKeys = new List<string>();
            _defaultParamKey = null;
        }

        public ParameterSetInfo(ParameterSetInfo parameterSetInfo)
        {
            _parameterMemberExpressions = new Dictionary<ExpressionComparer, MemberExpression[]>(parameterSetInfo._parameterMemberExpressions);
            _parameters = new Dictionary<string, Type>(parameterSetInfo._parameters);
            _parameterKeys = new List<string>(parameterSetInfo._parameterKeys);
            _defaultParamKey = parameterSetInfo._defaultParamKey;
        }

        public void ResetParameters()
        {
            _parameters.Clear();
        }

        public string RegisterParameter(string? name, Type t, bool asDefault)
        {
            string k0 = !string.IsNullOrEmpty(name) ? name! : "_t";
            string k = k0!;

            if (k != null)
            {
                int i = 1;
                while (_parameters.ContainsKey(k))
                {
                    k = $"{k0}{i++}";
                }
                _parameters.Add(k, t);
            }

            if (asDefault)
            {
                if (_defaultParamKey != null)
                {
                    _parameters.Remove(_defaultParamKey);
                    _defaultParamKey = null;
                }

                _defaultParamKey = k ?? _defaultParamKey;
            }
            else
            {
                _parameterKeys.Add(k!);
            }

            return k!;
        }

        public void SetParameterMemberExpressions(IDictionary<ExpressionComparer, MemberExpression[]> memberExpressions)
        {
            _parameterMemberExpressions = memberExpressions;
        }

        public Type GetTypeByName(string name)
        {
            return _parameters[name];
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
            //return _parameters.Select(x => (x.Key, x.Value)).OrderBy(x => x.Key == _defaultParamName ? -1 : 1);
            if (_defaultParamKey == null)
            {
                throw new InvalidOperationException();
            }
            return new[] { _defaultParamKey! }.Concat(_parameterKeys).Select(k => (k, _parameters[k]));
        }

        public Type? GetDefaultParamType()
        {
            return _defaultParamKey != null ? _parameters[_defaultParamKey] : null;
        }

        public string? GetDefaultParamName()
        {
            return _defaultParamKey;
        }

        private IDictionary<string, Type> _parameters;
        private IList<string> _parameterKeys;
        private IDictionary<ExpressionComparer, MemberExpression[]> _parameterMemberExpressions;
        private string? _defaultParamKey;
    }
}
