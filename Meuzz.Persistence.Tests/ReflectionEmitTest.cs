using System;
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

        public static Func<Character, Player> DefaultLoader = (x) =>
        {
            Console.WriteLine(x);
            return new Player() { Id = 999 };
        };

    }

    public class CharacterEx : Character
    {
        public new Player Player
        {
            get
            {
                var player = base.Player;
                if (player != null)
                {
                    return player;
                }

                //Func<Character, Player> f = CharacterExExtensions.DefaultLoader;

                //player = f(this);
                player = new Player();
                base.Player = player;
                /*
                return base.Player;*/
                // throw new NotImplementedException();

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

        [Fact]
        public void Test01()
        {
            Func<Character, Player> f = (x) =>
            {
                Console.WriteLine(x);
                return new Player() { Id = 999 };
            };

            var body = new ReflectionEmit();
            var t = body.CreateTypeOverride(typeof(Character), typeof(Character).GetPropertyInfo("Player"));

            dynamic obj = Activator.CreateInstance(t);
            _output.WriteLine(t.ToString());
            _output.WriteLine(obj.ToString());
            var p = obj.Player;
            _output.WriteLine(p.ToString());


        }

    }


}