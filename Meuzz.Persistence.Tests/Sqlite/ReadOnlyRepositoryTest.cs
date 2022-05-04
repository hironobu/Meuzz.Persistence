using System;
using System.Collections.Generic;
using System.Linq;
using Meuzz.Persistence.Tests.Models.ReadOnly;
using Xunit;

namespace Meuzz.Persistence.Tests.Sqlite
{
    public class ReadOnlyRepositoryTest
    {
        private IDatabaseContext _context;
        private ObjectRepository _repository;
        public ReadOnlyRepositoryTest()
        {
            var engine = DatabaseEngineFactory.Instance().GetEngine("sqlite");

            _context = engine.CreateContext("type=sqlite;file=:memory:");
            _context.Open();

            _context.Execute(@"
                CREATE TABLE Players (ID integer PRIMARY KEY, NAME text, AGE integer, PLAY_TIME integer);
                CREATE TABLE Characters (ID integer PRIMARY KEY, NAME text, PLAYER_ID integer, LAST_PLAYER_ID integer NULL, FOREIGN KEY (PLAYER_ID) REFERENCES Players(ID), FOREIGN KEY (LAST_PLAYER_ID) REFERENCES Players(ID));
            ");
            _context.Execute(@"
                INSERT INTO Players VALUES (1, 'aaa', 10, 100);
                INSERT INTO Players VALUES (2, 'bbb', 20, 200);
                INSERT INTO Players VALUES (3, 'ccc''s', 10, 200);
                INSERT INTO Characters VALUES (1, 'aaaa', 1, 3);
                INSERT INTO Characters VALUES (2, 'bbbb', 1, NULL);
                INSERT INTO Characters VALUES (3, 'cccc', 2, 3);
            ");

            _repository = new ObjectRepository();
        }

        [Fact]
        public void TestLoadById()
        {
            var objs = _repository.Load<Player>(_context, 1);
            Assert.Single(objs);
            Assert.Equal((Int64)1, objs.ElementAt(0).Id);
            var objs2 = _repository.Load<Player>(_context, 2);
            Assert.Single(objs2);
            Assert.Equal((Int64)2, objs2.ElementAt(0).Id);

            var objs3 = _repository.Load<Player>(_context, 1, 2, 3);
            Assert.Equal(3, objs3.Count());
            Assert.Equal((Int64)1, objs3.ElementAt(0).Id);
            Assert.Equal("aaa", objs3.ElementAt(0).Name);
            Assert.Equal((Int64)2, objs3.ElementAt(1).Id);
            Assert.Equal("bbb", objs3.ElementAt(1).Name);
            Assert.Equal((Int64)3, objs3.ElementAt(2).Id);
            Assert.Equal("ccc's", objs3.ElementAt(2).Name);
        }

        [Fact]
        public void TestLoadByLambda()
        {
            var objs = _repository.Load<Player>(_context, (x) => x.Name == "aaa");
            Assert.Single(objs);
            var objs2 = _repository.Load<Player>(_context, (x) => x.Age == 10);
            Assert.Equal(2, objs2.Count());
            Assert.Equal(1, objs2.ElementAt(0).Id);
            Assert.Equal(3, objs2.ElementAt(1).Id);
        }

        [Fact]
        public void TestLoadByLambdaAndVariables()
        {
            var c = "aaa";
            var objs = _repository.Load<Player>(_context, x => x.Name == c);
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);

            var age = 10;
            var objs2 = _repository.Load<Player>(_context, x => x.Age == age);
            Assert.Equal(2, objs2.Count());
            Assert.Equal(1, objs2.ElementAt(0).Id);
            Assert.Equal(3, objs2.ElementAt(1).Id);
            age = 20;
            Assert.Single(objs2);
            Assert.Equal(2, objs2.ElementAt(0).Id);
        }

        [Fact]
        public void TestLoadByClosure()
        {
            var c = "aaa";
            var objs = _repository.Load<Player>(_context, x => x.Name == c);
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);

            var age = 10;
            var objs2 = _repository.Load<Player>(_context, x => x.Age == age);
            Assert.Equal(2, objs2.Count());
            Assert.Equal(1, objs2.ElementAt(0).Id);
            Assert.Equal(3, objs2.ElementAt(1).Id);
            age = 20;
            Assert.Single(objs2);
            Assert.Equal(2, objs2.ElementAt(0).Id);
        }


        [Fact]
        public void TestLoadByLambdaNotEquals()
        {
            var objs = _repository.Load<Player>(_context, x => x.Name != "aaa");
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestLoadbyLambdaAnd()
        {
            var objs = _repository.Load<Player>(_context, x => x.Name == "aaa" && x.Age != 10);
            Assert.Empty(objs);
        }

        [Fact]
        public void TestLoadByLambdaWithJoins()
        {
            var objs = _repository.Load<Player>(_context, st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters, (x, r) => x.Id == r.Player.Id)
                .Joins(x => x.LastCharacters, (x, r) => x.Id == r.LastPlayer.Id));
            Assert.NotNull(objs);
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
            Assert.Empty(objs.ElementAt(0).LastCharacters);
            Assert.Equal(2, objs.ElementAt(1).LastCharacters.Count());
            Assert.Equal(1, objs.ElementAt(1).LastCharacters.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(1).LastCharacters.ElementAt(1).Id);
        }

        [Fact]
        public void TestLoadByLambdaWithJoinsWithoutId()
        {
            var objs = _repository.Load<Player>(_context, st => st.Where(x => x.Age == 10)
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
        public void TestLoadByLambdaWithJoinsAndHasMany()
        {
            var objs = _repository.Load<Player>(_context, st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
        }

        [Fact]
        public void TestLoadByLambdaWithJoinsAndHasManyOnPlayer2()
        {
            var objs = _repository.Load<Models.NoForeignKeyProperty.Player>(_context, st => st.Where(x => x.Age == 10)
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
        public void TestLoadByLambdaWithJoinsAndHasManyOnPlayer3()
        {
            var objs = _repository.Load<Models.AutoForeignKey.Player>(_context, st => st.Where(x => x.Age == 10)
                .Joins(x => x.Characters));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
        }

        [Fact]
        public void TestLoadByLambdaWithSelect()
        {
            var playerRepository = new ObjectRepository<Player>();

            var objs = playerRepository.Load(_context, st => st.Where(x => x.Age == 10).Select(x => x.Age));
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestLoadByLambdaWithSelect2()
        {
            var playerRepository = new ObjectRepository<Player>();

            var objs = playerRepository.Load(_context, st => st.Where(x => x.Age == 10).Select(x => new { Name = x.Name, Age = x.Age }));
            Assert.Equal(2, objs.Count());
        }

#if false
        [Fact]
        public void TestCreateAndUpdate()
        {
            var p = new Player(default, "Create Test", 999, default);
            var q = new Player(default, "Create Test 2", 10000, default);
            var r = new Player(1, "Update Test", default, default);

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

            _repository.Store(_context, new[] { p, q, r });

            var rset = _context.Execute("SELECT * FROM Players");

            Assert.Equal(5, rset.Results.Count());
            Assert.Equal("Update Test", rset.Results.ElementAt(0)["name"]);
            Assert.Equal((Int64)10, rset.Results.ElementAt(0)["age"]);
            Assert.Equal((Int64)100, rset.Results.ElementAt(0)["play_time"]);
            Assert.Equal("bbb", rset.Results.ElementAt(1)["name"]);
            Assert.Equal("ccc's", rset.Results.ElementAt(2)["name"]);
            Assert.Equal("Create Test", rset.Results.ElementAt(3)["name"]);
            Assert.Equal((Int64)999, rset.Results.ElementAt(3)["age"]);
            Assert.Equal("Create Test 2", rset.Results.ElementAt(4)["name"]);
            Assert.Equal((Int64)10000, rset.Results.ElementAt(4)["play_time"]);

            Assert.Equal(4, p.Id);
            Assert.Equal(5, q.Id);
            Assert.Equal(1, r.Id);

            var rset2 = _context.Execute("SELECT * FROM Characters");
            Assert.Equal(9, rset2.Results.Count());
        }

        [Fact]
        public void TestCreateBy10000ItemsWithRawSQL()
        {
            var sql = "INSERT INTO Characters (name) VALUES ";
            var values = new List<string>();
            for (int i = 0; i < 10000; i++)
            {
                values.Add($"('Char {i}')");
            };

            _context.Execute(sql + string.Join(", ", values));

            var rset = _context.Execute("SELECT * FROM Characters");
            Assert.Equal(10003, rset.Results.Count());
        }

        [Fact]
        public void TestCreateBy10000Items()
        {
            var characters = new List<Character>();
            for (int i = 0; i < 10000; i++)
            {
                characters.Add(new Character() { Name = $"Char {i}" });
            };

            _repository.Store(_context, characters.ToArray());

            var rset = _context.Execute("SELECT * FROM Characters");
            Assert.Equal(10003, rset.Results.Count());
        }

        [Fact]
        public void TestDelete()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(4, rset2.Results.Count());

            _repository.Delete<Player>(_context, (x) => x.Id == 4);
            // _repository.Delete(4);

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
        }

        [Fact]
        public void TestDeleteById()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(4, rset2.Results.Count());

            _repository.Delete<Player>(_context, 4);

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }

        [Fact]
        public void TestDeleteByLamdaAndName()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });
            _repository.Store(_context, new Player() { Name = "yyy" });
            _repository.Store(_context, new Player() { Name = "zzz" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            _repository.Delete<Player>(_context, x => x.Name == "yyy");

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(5, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
            Assert.Equal((Int64)4, rset3.Results.ElementAt(3)["id"]);
            Assert.Equal((Int64)6, rset3.Results.ElementAt(4)["id"]);
        }

        [Fact]
        public void TestDeleteByLamdaAndName2()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });
            _repository.Store(_context, new Player() { Name = "yyy" });
            _repository.Store(_context, new Player() { Name = "zzz" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            var names = new[] { "xxx", "yyy", "zzz" };
            _repository.Delete<Player>(_context, x => names.Contains(x.Name));

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }


        [Fact]
        public void TestDeleteByIds()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });
            _repository.Store(_context, new Player() { Name = "yyy" });
            _repository.Store(_context, new Player() { Name = "zzz" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            _repository.Delete<Player>(_context, 4, 5, 6);

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }

        [Fact]
        public void TestDeleteByIdsWithExpression()
        {
            var rset = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(_context, new Player() { Name = "xxx" });
            _repository.Store(_context, new Player() { Name = "yyy" });
            _repository.Store(_context, new Player() { Name = "zzz" });

            var rset2 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            var ids = new int[] { 4, 5, 6 };
            _repository.Delete<Player>(_context, x => ids.Contains(x.Id));

            var rset3 = _context.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }
#endif
    }
}