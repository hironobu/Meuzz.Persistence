using System;
using System.Collections.Generic;
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
            var n = 0;

            CharacterEx.NewPlayer = (x) =>
            {
                for (var i = 0; i < n; i++)
                {
                    Console.WriteLine(x);
                }
                return new Player() { Id = 999 };
            };
        }

        public object Run()
        {
            BeforeRun();

            var ch = new CharacterEx();

            Console.WriteLine("<<<");
            var n2 = ch.Player;
            Console.WriteLine("<<< " + n2);
            return n2;
        }
    }

    public class CharacterEx : Character
    {
        public static Func<Character, Player> NewPlayer = null;

        public new Player Player
        {
            get
            {
                var player = base.Player;
                if (player != null)
                {
                    return player;
                }

                player = NewPlayer(this);
                base.Player = player;
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

            var body = new ReflectionEmit();
            body.BuildStart(Assembly.GetExecutingAssembly().GetName(), typeof(Character));
            body.BuildProperty(typeof(Character).GetPropertyInfo("Player"), ff);
            var t = body.BuildFinish();

            dynamic obj = Activator.CreateInstance(t);
            _output.WriteLine(t.ToString());
            _output.WriteLine(obj.ToString());
            var p = obj.Player;
            _output.WriteLine(p.ToString());
            var p2 = obj.Player;
            _output.WriteLine(p2.ToString());


        }

    }


}