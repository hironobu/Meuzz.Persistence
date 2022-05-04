using System;
using System.Collections.Generic;
using System.Data;
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

        public static IDatabaseContext CreateContext(this IDatabaseEngine self, string connectionString)
        {
            return self.CreateContext(ParseContextString(connectionString));
        }
    }

    public class DatabaseEngineFactory
    {
        private IDictionary<string, IDatabaseEngine> _engines = new Dictionary<string, IDatabaseEngine>();

        public void Initialize()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                CallServiceProvidersOnAssembly(assembly);
            }
        }

        public void Register(string name, IDatabaseEngine engine)
        {
            _engines.Add(name, engine);
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

        public IDatabaseEngine GetEngine(string type)
        {
            if (!_engines.TryGetValue(type.ToString(), out var engine))
            {
                throw new NotImplementedException();
            }

            return engine;
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
