using System;
using System.Reflection;
using Xunit;
using Xunit.Abstractions;

namespace Meuzz.Persistence.Tests
{
    public class CharacterExExtensions
    {
        /*public static T1 DummyLoader<T, T1>(T obj) where T1 : new()
        {
            return new T1();
        }*/
        public void BeforeRun()
        {
        }

        public object Run()
        {
            BeforeRun();
            var n = 0;


            var ch = new CharacterEx();
            ch.NewPlayer = (x) =>
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

    public class CharacterEx : Character
    {
        public Func<Character, Player> NewPlayer = null;

        public new Player Player
        {
            get
            {
                var player = base.Player;
                if (player != null)
                {
                    return player;
                }

                if (NewPlayer != null)
                {
                    player = NewPlayer(this);
                    base.Player = player;
                }
                return player;
            }
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

            var body = new ProxyTypeBuilder();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            dynamic obj = Activator.CreateInstance(t);
            var field = obj.GetType().GetField("__PlayerLoader");
            field.SetValue(obj, ff);

            dynamic obj2 = Activator.CreateInstance(t);
            var field2 = obj2.GetType().GetField("__PlayerLoader");
            field2.SetValue(obj2, ff2);

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
            var body = new ProxyTypeBuilder();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            dynamic obj = Activator.CreateInstance(t);

            dynamic obj2 = Activator.CreateInstance(t);
            var field2 = obj2.GetType().GetField("__PlayerLoader");
            field2.SetValue(obj2, null);

            _output.WriteLine(obj.ToString());
            var p = obj.Player;
            Assert.Null(p);
            var p2 = obj2.Player;
            Assert.Null(p);
        }


        [Fact]
        public void TestEquality()
        {
            var body = new ProxyTypeBuilder();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t = body.BuildFinish();

            var body2 = new ProxyTypeBuilder();
            body2.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body2.BuildOverrideProperty(typeof(Character).GetPropertyInfo("Player"));
            var t2 = body2.BuildFinish();

            Assert.NotEqual(t, t2);
            Assert.NotSame(t, t2);
        }
    }

}