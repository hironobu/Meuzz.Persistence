using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Meuzz.Persistence.Tests
{
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

        [HasMany(typeof(Character), foreignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; set; }

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

        public Player LastPlayer { get; set; }
    }

    public class Geometry
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }

        public double Altitude { get; set; }
    }


    [PersistentClass("Players")]
    public class Player2
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany(typeof(Character2), foreignKey: "player_id")]
        public IEnumerable<Character2> Characters { get; set; }

        [HasMany(typeof(Character2), foreignKey: "last_player_id")]
        public IEnumerable<Character2> LastCharacters { get; set; }
    }

    [PersistentClass("Characters")]
    public class Character2
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public Geometry Location { get; set; }
    }



    public class RepositoryTest
    {
        private Connection _connection;
        private ObjectRepository<Player> _repository;

        public RepositoryTest()
        {
            _connection = new SqliteConnectionImpl("dummy.sqlite");
            _connection.Open();

            _connection.Execute(@"
                CREATE TABLE Players (ID integer PRIMARY KEY, NAME text, AGE integer, PLAY_TIME integer);
                CREATE TABLE Characters (ID integer PRIMARY KEY, NAME text, PLAYER_ID integer, LAST_PLAYER_ID integer NULL, FOREIGN KEY (PLAYER_ID) REFERENCES Players(ID), FOREIGN KEY (LAST_PLAYER_ID) REFERENCES Players(ID));
            ");
            _connection.Execute(@"
                INSERT INTO Players VALUES (1, 'aaa', 10, 100);
                INSERT INTO Players VALUES (2, 'bbb', 20, 200);
                INSERT INTO Players VALUES (3, 'ccc', 10, 200);
                INSERT INTO Characters VALUES (1, 'aaaa', 1, 3);
                INSERT INTO Characters VALUES (2, 'bbbb', 1, NULL);
                INSERT INTO Characters VALUES (3, 'cccc', 2, 3);
            ");
            _repository = new ObjectRepository<Player>(_connection, new SqliteSqlBuilder<Player>(), new SqliteFormatter(), new SqliteCollator());

            Console.WriteLine("OK");
        }

        [Fact]
        public void TestLoadById()
        {
            var objs = _repository.Load(1);
            Assert.Single(objs);
            Assert.Equal((Int64)1, objs.ElementAt(0).Id);
            var objs2 = _repository.Load(2);
            Assert.Single(objs2);
            Assert.Equal((Int64)2, objs2.ElementAt(0).Id);

            var objs3 = _repository.Load(1, 2, 3);
            Assert.Equal(3, objs3.Count());
            Assert.Equal((Int64)1, objs3.ElementAt(0).Id);
            Assert.Equal("aaa", objs3.ElementAt(0).Name);
            Assert.Equal((Int64)2, objs3.ElementAt(1).Id);
            Assert.Equal("bbb", objs3.ElementAt(1).Name);
            Assert.Equal((Int64)3, objs3.ElementAt(2).Id);
            Assert.Equal("ccc", objs3.ElementAt(2).Name);
        }

        [Fact]
        public void TestWhereEquals()
        {
            var objs = _repository.Load((x) => x.Name == "aaa");
            Assert.Single(objs);
            var objs2 = _repository.Load((x) => x.Age == 10);
            Assert.Equal(2, objs2.Count());
        }

        [Fact]
        public void TestWhereNotEquals()
        {
            var objs = _repository.Load(x => x.Name != "aaa");
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestWhereAnd()
        {
            var objs = _repository.Load(x => x.Name == "aaa" && x.Age != 10);
            Assert.Empty(objs);
        }

        [Fact]
        public void TestWhereEqualsAndJoins()
        {
            var objs = _repository.Load(st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters, (x, r) => x.Id == r.Player.Id)
                .Joins(x => x.LastCharacters, (x, r) => x.Id == r.LastPlayer.Id));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
            Assert.Empty(objs.ElementAt(0).LastCharacters);
            Assert.Equal(2, objs.ElementAt(1).LastCharacters.Count());
            Assert.Equal(1, objs.ElementAt(1).LastCharacters.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(1).LastCharacters.ElementAt(1).Id);
        }

        [Fact]
        public void TestWhereEqualsAndJoinsWithoutId()
        {
            var objs = _repository.Load(st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters, (x, r) => x == r.Player)
                .Joins(x => x.LastCharacters, (x, r) => x == r.LastPlayer));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
            Assert.Empty(objs.ElementAt(0).LastCharacters);
            Assert.Equal(2, objs.ElementAt(1).LastCharacters.Count());
            Assert.Equal(1, objs.ElementAt(1).LastCharacters.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(1).LastCharacters.ElementAt(1).Id);
        }

        [Fact]
        public void TestWhereEqualsAndJoinsByHasMany()
        {
            var objs = _repository.Load(st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
        }

        [Fact]
        public void TestWhereEqualsAndJoinsByHasManyOnPlayer2()
        {
            var objs = _repository.Load(st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters)
                .Joins(x => x.LastCharacters));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
            Assert.Empty(objs.ElementAt(0).LastCharacters);
            Assert.Equal(2, objs.ElementAt(1).LastCharacters.Count());
            Assert.Equal(1, objs.ElementAt(1).LastCharacters.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(1).LastCharacters.ElementAt(1).Id);
        }

        [Fact]
        public void TestCreateAndUpdate()
        {
            var p = new Player() { Name = "Create Test", Age = 999 };
            var q = new Player() { Name = "Create Test 2", PlayTime = 10000 };
            var r = new Player() { Id = 1, Name = "Update Test" };

            p.Characters = new[]
            {
                new Character() { Name = "Char 1" },
                new Character() { Name = "Char 2" }
            };
            p.LastCharacters = new[]
            {
                new Character() { Name = "Char 11" },
                new Character() { Name = "Char 12" },
                new Character() { Name = "Char 13" },
                new Character() { Name = "Char 14" }
            };

            _repository.Store(new[] { p, q, r });

            var rset = _connection.Execute("SELECT * FROM Players");

            Assert.Equal(5, rset.Results.Count());
            Assert.Equal("Update Test", rset.Results.ElementAt(0)["name"]);
            Assert.Equal("bbb", rset.Results.ElementAt(1)["name"]);
            Assert.Equal("ccc", rset.Results.ElementAt(2)["name"]);
            Assert.Equal("Create Test", rset.Results.ElementAt(3)["name"]);
            Assert.Equal((Int64)999, rset.Results.ElementAt(3)["age"]);
            Assert.Equal("Create Test 2", rset.Results.ElementAt(4)["name"]);
            Assert.Equal((Int64)10000, rset.Results.ElementAt(4)["play_time"]);

            var rset2 = _connection.Execute("SELECT * FROM Characters");
            Assert.Equal(9, rset2.Results.Count());
        }

        [Fact]
        public void TestDelete()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(4, rset2.Results.Count());

            _repository.Delete((x) => x.Id == 4);
            // _repository.Delete(4);

            var rset3 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
        }

        [Fact]
        public void TestDeleteById()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(4, rset2.Results.Count());

            _repository.Delete(4);

            var rset3 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }

        [Fact]
        public void TestDeleteByIds()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });
            _repository.Store(new Player() { Name = "yyy" });
            _repository.Store(new Player() { Name = "zzz" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            _repository.Delete(4, 5, 6);

            var rset3 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }

        [Fact]
        public void TestDeleteByIdsWithExpression()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });
            _repository.Store(new Player() { Name = "yyy" });
            _repository.Store(new Player() { Name = "zzz" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            var ids = new int[] { 4, 5, 6 };
            _repository.Delete(x => ids.Contains(x.Id));

            var rset3 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }

    }
}