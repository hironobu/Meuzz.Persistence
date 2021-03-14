﻿using System;
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
            field.SetValue(obj2, ff2);

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

        }

    }


}