using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public static class DatabaseEngineExtensions
    {
        private static IDictionary<string, object> ParseContextString(string connectionString)
        {
            return connectionString.Split(";").Select(x => x.Split("=", 2)).ToDictionary(x => x[0], x => (object)x[1]);
        }

        /*
        public static IDatabaseContext CreateContext(this IDatabaseEngine self, string connectionString)
        {
            return self.CreateContext(ParseContextString(connectionString));
        }
        */
    }

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
                throw new NotSupportedException();
            }

            _engines.Add(name, engineType);
        }

        public IDatabaseEngine GetEngine(string type, string connectionString)
        {
            if (!_engines.TryGetValue(type.ToString(), out var engineType))
            {
                throw new NotImplementedException();
            }

            var engine = (IDatabaseEngine)Activator.CreateInstance(engineType);
            engine.Configure(connectionString);
            return engine;
        }

        public IDatabaseEngine GetEngine(string type, DbConnectionStringBuilder dbConnectionStringBuilder)
        {
            return GetEngine(type, dbConnectionStringBuilder.ConnectionString);
        }

        private void CallServiceProvidersOnAssembly(Assembly asm)
        {
            foreach (var type in asm.GetTypes())
            {
                if (typeof(IDatabaseEngineProvider).IsAssignableFrom(type) && !(type == typeof(IDatabaseEngineProvider)))
                {
                    var provider = Activator.CreateInstance(type) as IDatabaseEngineProvider;
                    provider.Register(this);
                }
            }
        }

        private IDictionary<string, object> ParseConnectionString(string connectionString)
        {
            return connectionString.Split(";").Select(x => x.Split("=", 2)).ToDictionary(x => x[0], x => (object)x[1]);
        }

        private static DatabaseEngineFactory _instance;

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
