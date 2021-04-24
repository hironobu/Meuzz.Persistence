using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Meuzz.Persistence.Reflections;
using Xunit;
using Xunit.Abstractions;

namespace Meuzz.Persistence.Tests
{
    public class CharacterExExtensions
    {
        public void BeforeRun()
        {
        }

        public object Run()
        {
            BeforeRun();
            var n = 0;

            var ch = new CharacterEx();
            ch.__Loader__.Player = (x) =>
            {
                for (var i = 0; i < n; i++)
                {
                    Console.WriteLine(x);
                }
                return new Player() { Id = 999 };
            };

            Console.WriteLine("<<<");
            var n2 = ch.Player;
            Console.WriteLine("<<< " + n2);
            return n2;
        }
    }

    public class TestMain
    {
        public static void Run()
        {
            var c = new CharacterEx() { Id = 111, Name = "aaa" };

            Console.WriteLine("HOGEHOGE");
            Console.WriteLine($"Name: {c.Name}");
        }
    }

    public class CharacterEx : Character, IPersistable
    {
        public class Loader
        {
            public Func<Character, Player> Player = null;
        }
        public Loader __Loader__ = new Loader();

        private Player __player = null;
        private IDictionary<string, bool> __dirty = new Dictionary<string, bool>();

        public new Player Player
        {
            set
            {
                if (__player != value)
                {
                    __player = value;
                    if (__dirty != null)
                    {
                        __dirty["Player"] = true;
                    }
                }
            }

            get
            {
                var player = __player;
                if (player != null)
                {
                    return player;
                }

                if (__Loader__.Player != null)
                {
                    player = __Loader__.Player(this);
                    __player = player;
                }
                return player;
            }
        }

        public PersistableState GeneratePersistableState()
        {
            var keys = __dirty.Keys.ToArray();
            __dirty.Clear();
            return new PersistableState(keys);
        }
    }

    public class ReflectionEmitTest
    {
        private readonly ITestOutputHelper _output;

        public ReflectionEmitTest(ITestOutputHelper output)
        {
            _output = output;
        }

        public static Player DummyLoader(Character x)
        {
            Console.WriteLine(x);
            return new Player() { Id = 999 };
        }

        [Fact]
        public void Test01()
        {
            Func<Character, Player> ff = (c) =>
            {
                Console.WriteLine(c);
                return new Player() { Id = 999 };
            };

            Func<Character, Player> ff2 = (c) =>
            {
                Console.WriteLine(c);
                return new Player() { Id = 11111 };
            };

            var body = new PersistentTypeBuilder();
            body.BuildStart(typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            dynamic obj = Activator.CreateInstance(t);
            var loaderField = obj.GetType().GetField("__Loader__");
            var loader = loaderField.GetValue(obj);
            var field = loader.GetType().GetField("Player");
            field.SetValue(loader, ff);

            dynamic obj2 = Activator.CreateInstance(t);
            var loaderField2 = obj2.GetType().GetField("__Loader__");
            var loader2 = loaderField2.GetValue(obj2);
            var field2 = loader2.GetType().GetField("Player");
            field2.SetValue(loader2, ff2);

            _output.WriteLine(t.ToString());
            _output.WriteLine(obj.ToString());
            var p = obj.Player;
            _output.WriteLine(p.ToString());
            Assert.Equal(999, p.Id);
            var p2 = obj.Player;
            _output.WriteLine(p2.ToString());
            Assert.Equal(999, p2.Id);

            p = obj2.Player;
            _output.WriteLine(p.ToString());
            Assert.Equal(11111, p.Id);
            p2 = obj2.Player;
            _output.WriteLine(p2.ToString());
            Assert.Equal(11111, p2.Id);

            p = obj.Player;
            _output.WriteLine(p.ToString());
            Assert.Equal(999, p.Id);
            p2 = obj.Player;
            _output.WriteLine(p2.ToString());
            Assert.Equal(999, p2.Id);
        }

        [Fact]
        public void TestNullLoader()
        {
            var body = new PersistentTypeBuilder();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            dynamic obj = Activator.CreateInstance(t);

            dynamic obj2 = Activator.CreateInstance(t);
            var loaderField2 = obj2.GetType().GetField("__Loader__");
            var loader2 = loaderField2.GetValue(obj2);
            var field2 = loader2.GetType().GetField("Player");
            field2.SetValue(loader2, null);

            _output.WriteLine(obj.ToString());
            var p = obj.Player;
            Assert.Null(p);
            var p2 = obj2.Player;
            Assert.Null(p);
        }


        [Fact]
        public void TestEquality()
        {
            var body = new PersistentTypeBuilder();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            var body2 = new PersistentTypeBuilder();
            body2.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body2.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t2 = body2.BuildFinish();

            Assert.NotEqual(t, t2);
            Assert.NotSame(t, t2);
        }
    }

}