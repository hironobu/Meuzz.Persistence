using Meuzz.Persistence.Sql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence
{
    public static class PersistenceEngineExtensions
    {
        private static IDictionary<string, object> ParseContextString(string connectionString)
        {
            return connectionString.Split(";").Select(x => x.Split("=", 2)).ToDictionary(x => x[0], x => (object)x[1]);
        }

        public static IPersistenceContext CreateContext(this IPersistenceEngine self, string connectionString)
        {
            return self.CreateContext(ParseContextString(connectionString));
        }
    }

    public class PersistenceEngineFactory
    {
        private IDictionary<string, IPersistenceEngine> _engines = new Dictionary<string, IPersistenceEngine>();

        public void Initialize()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                CallServiceProvidersOnAssembly(assembly);
            }
        }

        public void Register(string name, IPersistenceEngine engine)
        {
            _engines.Add(name, engine);
        }

        private void CallServiceProvidersOnAssembly(Assembly asm)
        {
            foreach (var type in asm.GetTypes())
            {
                if (typeof(IPersistenceEngineProvider).IsAssignableFrom(type) && !(type == typeof(IPersistenceEngineProvider)))
                {
                    var provider = Activator.CreateInstance(type) as IPersistenceEngineProvider;
                    provider.Register(this);
                }
            }
        }

        public IPersistenceEngine GetEngine(string type)
        {
            if (!_engines.TryGetValue(type.ToString(), out var engine))
            {
                throw new NotImplementedException();
            }

            return engine;
        }

        /*public Connection NewConnection(string connectionString)
        {
            var parameters = ParseConnectionString(connectionString);

            if (!_engines.TryGetValue(parameters["type"].ToString(), out var engine))
            {
                throw new NotImplementedException();
            }

            return engine.CreateConnection(parameters);
        }*/

        private IDictionary<string, object> ParseConnectionString(string connectionString)
        {
            return connectionString.Split(";").Select(x => x.Split("=", 2)).ToDictionary(x => x[0], x => (object)x[1]);
        }

        /*
        public SqlFormatter NewFormatter(string type)
        {
            if (!_engines.TryGetValue(type, out var engine))
            {
                throw new NotImplementedException();
            }

            return engine.CreateFormatter();
        }*/


        private static PersistenceEngineFactory _instance;
        public static PersistenceEngineFactory Instance()
        {
            if (_instance == null)
            {
                var instance = new PersistenceEngineFactory();

                instance.Initialize();
                _instance = instance;
            }
            return _instance;
        }
    }
}
