using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public class DatabaseEngineFactory
    {
        private IDictionary<string, Type> _engines = new Dictionary<string, Type>();

        public void Initialize()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                CallServiceProvidersOnAssembly(assembly);
            }
        }

        public void Register(string name, Type engineType)
        {
            if (!typeof(IDatabaseEngine).IsAssignableFrom(engineType))
            {
                throw new ArgumentException($"Not Acceptable Type: {engineType}");
            }

            _engines.Add(name, engineType);
        }

        public IDatabaseEngine CreateEngine(string type, string connectionString)
        {
            if (!_engines.TryGetValue(type.ToString(), out var engineType))
            {
                throw new ArgumentException($"Not Registerd for Type: {type}");
            }

            var engine = Activator.CreateInstance(engineType) as IDatabaseEngine;
            if (engine == null)
            {
                throw new ArgumentException("CreateInstance() failed for Type: {type}");
            }
            engine.Configure(connectionString);
            return engine;
        }

        public IDatabaseEngine CreateEngine(string type, DbConnectionStringBuilder dbConnectionStringBuilder)
        {
            return CreateEngine(type, dbConnectionStringBuilder.ConnectionString);
        }

        private void CallServiceProvidersOnAssembly(Assembly asm)
        {
            foreach (var type in asm.GetTypes())
            {
                if (typeof(IDatabaseEngineProvider).IsAssignableFrom(type) && !(type == typeof(IDatabaseEngineProvider)))
                {
                    var provider = Activator.CreateInstance(type) as IDatabaseEngineProvider;
                    provider?.Register(this);
                }
            }
        }

        private static DatabaseEngineFactory _instance = default!;

        public static DatabaseEngineFactory Instance()
        {
            if (_instance == null)
            {
                var instance = new DatabaseEngineFactory();

                instance.Initialize();
                _instance = instance;
            }
            return _instance;
        }
    }
}
