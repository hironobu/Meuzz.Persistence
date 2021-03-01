using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class TypeExtensions
    {
        public static Type FindTypeFromClassName(Type self, Assembly assembly, string className)
        {
            var ts = assembly.GetTypes();

            return ts.Single(x => x.Name == className);
        }

    }


    [PersistentClass("Players")]
    public class Player
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany(typeof(Character), foreignKey: "player_id")]
        public IEnumerable<Character> Characters { get; set; }

        public IEnumerable<Item> Items { get; set; }
    }

    public class Item
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }
    }


    [PersistentClass("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public Geometry Location { get; set; }

        public Player Player { get; set; }
    }

    public class Geometry
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }

        public double Altitude { get; set; }
    }



    public class PersistenceTest
    {
        private Connection _connection;
        private ObjectRepository<Player, int> _repository;

        public PersistenceTest()
        {
            _connection = new SqliteConnectionImpl("dummy.sqlite");
            _repository = new ObjectRepository<Player, int>(_connection, new SqliteSqlBuilder<Player>(), new SqliteFormatter());
            _connection.Open();

            _connection.Execute(@"
                CREATE TABLE Players (ID integer AUTO_INCREMENT, NAME text, AGE integer, PLAY_TIME integer);
                INSERT INTO Players VALUES (1, 'aaa', 10, 100);
                INSERT INTO Players VALUES (2, 'bbb', 20, 200);
                INSERT INTO Players VALUES (3, 'ccc', 10, 200);
                CREATE TABLE Characters (ID integer AUTO_INCREMENT, NAME text, PLAYER_ID integer);
                INSERT INTO Characters VALUES (1, 'aaaa', 1);
                INSERT INTO Characters VALUES (2, 'bbbb', 1);
                INSERT INTO Characters VALUES (3, 'cccc', 2);
            ");

            Console.WriteLine("OK");
        }

        [Fact]
        public void TestWhereEquals()
        {
            var objs = _repository.Where((x) => x.Name == "aaa").Execute();
            Assert.Single(objs);
            var objs2 = _repository.Where((x) => x.Age == 10).Execute();
            Assert.Equal(2, objs2.Count());
        }

        [Fact]
        public void TestWhereNotEquals()
        {
            var objs = _repository.Where((x) => x.Name != "aaa").Execute();
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestWhereAnd()
        {
            var objs = _repository.Where((x) => x.Name == "aaa" && x.Age != 10).Execute();
            Assert.Empty(objs);
        }

        [Fact]
        public void TestWhereEqualsAndIncludes()
        {
            var t = new Player() { Characters = null };
            var q = _repository.Where((x) => x.Age == 10)
                .Joins(x => x.Characters, (l, r) => r.Player.Id == l.Id);
            var objs = q.Execute();
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
        }

        /*[Fact]
        public void TestWherePropertyTree()
        {
            var objs = _repository.Where((x) => x.Name == "aaa");

            Console.WriteLine("OK");
        }*/

        /*
                [Fact]
                public void Test2()
                {
                    var obj = _repository.Create();
                    obj.Name = "hoge";
                    obj.Save();

                    Console.WriteLine("OK");
                }*/
    }
}