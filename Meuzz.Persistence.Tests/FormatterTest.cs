using Meuzz.Persistence.Database;
using Meuzz.Persistence.Sql;
using Meuzz.Persistence.Sqlite;
using Meuzz.Persistence.Tests.Models;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class FormatterTest
    {
        private IDatabaseContext _context;
        /*private ObjectRepository<Player> _repository;
        private ObjectRepository<Character> _characterRepository;*/

        // private SqlFormatter _formatter = null;

        public FormatterTest()
        {
            var engine = DatabaseEngineFactory.Instance().CreateEngine("sqlite", "Data Source=:memory:");

            _context = engine.CreateContext();
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

            // _formatter = engine.CreateFormatter();

/*            _repository = new ObjectRepository<Player>(_connection, new SqliteSqlBuilder<Player>(), new SqliteFormatter(), new SqliteCollator());
            _characterRepository = new ObjectRepository<Character>(_connection, new SqliteSqlBuilder<Character>(), new SqliteFormatter(), new SqliteCollator());

            Console.WriteLine("OK");*/
        }

        [Fact]
        public void TestLoadById()
        {
            var formatter = new SqliteFormatter();

            //var statement = new SelectStatement<Player>();
            //var objs = formatter.Format(statement);

            //Assert.Equal("SELECT _t0.id AS _c0, _t0.name AS _c1, _t0.age AS _c2, _t0.play_time AS _c3 FROM Players x", objs.Sql);
            //Assert.Null(objs.Parameters);

            var statement1 = new SelectStatement<Player>();
            statement1 = statement1.Where("id", 1);

            var objs1 = formatter.Format(statement1);

            Assert.Empty(statement1.ColumnSpecs);
            Assert.Empty(statement1.RelationSpecs);

            Assert.Equal("SELECT _t0.id AS _c0, _t0.name AS _c1, _t0.age AS _c2, _t0.play_time AS _c3 FROM Players _t0 WHERE (_t0.id) = (1)", objs1.Sql);
            Assert.Null(objs1.Parameters);

            var statement2 = new SelectStatement<Player>();
            statement2 = statement2.Where("id", 2);
            var objs2 = formatter.Format(statement2);

            Assert.Equal("SELECT _t0.id AS _c0, _t0.name AS _c1, _t0.age AS _c2, _t0.play_time AS _c3 FROM Players _t0 WHERE (_t0.id) = (2)", objs2.Sql);
            Assert.Null(objs2.Parameters);

            var statement3 = new SelectStatement<Player>();
            statement3 = statement3.Where("id", 1, 2, 3);
            var objs3 = formatter.Format(statement3);

            Assert.Equal("SELECT _t0.id AS _c0, _t0.name AS _c1, _t0.age AS _c2, _t0.play_time AS _c3 FROM Players _t0 WHERE (_t0.id) IN (1, 2, 3)", objs3.Sql);
            Assert.Null(objs3.Parameters);
        }

        [Fact]
        public void TestLoadByIdAndSelect()
        {
            var formatter = new SqliteFormatter();

            var statement = new SelectStatement<Player>().Where("id", 1, 2, 3);
            var objs = formatter.Format(statement);
            var statement2 = statement.Select(x => new { Id = x.Id, HowLong = x.PlayTime });
            var objs2 = formatter.Format(statement2);

            Assert.Equal("SELECT _t0.id AS _c0, _t0.name AS _c1, _t0.age AS _c2, _t0.play_time AS _c3 FROM Players _t0 WHERE (_t0.id) IN (1, 2, 3)", objs.Sql);
            Assert.Null(objs.Parameters);
            Assert.Equal(4, objs.ColumnCollationInfo.GetAliases().Length);
            Assert.Equal("_t0.id", objs.ColumnCollationInfo._GetOriginalColumnName("_c0"));
            Assert.Equal("_t0.name", objs.ColumnCollationInfo._GetOriginalColumnName("_c1"));
            Assert.Equal("_t0.age", objs.ColumnCollationInfo._GetOriginalColumnName("_c2"));
            Assert.Equal("_t0.play_time", objs.ColumnCollationInfo._GetOriginalColumnName("_c3"));
            Assert.Equal("_t0.play_time", objs.ColumnCollationInfo.GetOutputColumnName("_c3"));

            Assert.Equal("SELECT _t0.id AS _c0, _t0.play_time AS _c1 FROM Players _t0 WHERE (_t0.id) IN (1, 2, 3)", objs2.Sql);
            Assert.Null(objs2.Parameters);
            Assert.Equal(2, objs2.ColumnCollationInfo.GetAliases().Length);
            Assert.Equal("_t0.id", objs2.ColumnCollationInfo._GetOriginalColumnName("_c0"));
            Assert.Equal("_t0.play_time", objs2.ColumnCollationInfo._GetOriginalColumnName("_c1"));
            Assert.Equal("_t0.how_long", objs2.ColumnCollationInfo.GetOutputColumnName("_c1"));
        }

        [Fact]
        public void TestLoadByIdAndSelectBySingleColumn()
        {
            var formatter = new SqliteFormatter();

            var statement = new SelectStatement<Player>().Where("id", 1, 2, 3).Select(x => x.Age);
            var objs2 = formatter.Format(statement);

            Assert.Equal("SELECT _t0.age AS _c0 FROM Players _t0 WHERE (_t0.id) IN (1, 2, 3)", objs2.Sql);
            Assert.Null(objs2.Parameters);
            Assert.Equal("_t0.age", objs2.ColumnCollationInfo._GetOriginalColumnName("_c0"));
            Assert.Equal("_t0.age", objs2.ColumnCollationInfo.GetOutputColumnName("_c0"));
        }

        [Fact]
        public void TestLoadByIdAndJoinsAndSelect()
        {
            var formatter = new SqliteFormatter();

            var statement = new SelectStatement<Player>().Where("id", 1, 2, 3);
            var statement2 = statement.Joins(p => p.Characters).Select(x => new { Id = x.Id, HowLong = x.PlayTime });
            var objs4 = formatter.Format(statement2);
            Assert.Equal("SELECT _t0.id AS _c0, _t0.play_time AS _c1 FROM Players _t0 LEFT JOIN Characters _t1 ON _t0.id = _t1.player_id WHERE (_t0.id) IN (1, 2, 3)", objs4.Sql);
            Assert.Null(objs4.Parameters);
            Assert.Equal("_t0.id", objs4.ColumnCollationInfo._GetOriginalColumnName("_c0"));
        }

        [Fact]
        public void TestUpdate()
        {
            var formatter = new SqliteFormatter();

            var obj = new Player() { Id = 1 };
            PersistableState.Reset(obj);

            obj.Name = "aaa";

            var statement = new UpdateStatement<Player>();
            statement.Append(new[] { obj });

            var update = formatter.Format(statement);
            Assert.Equal("UPDATE Players SET name = 'aaa' WHERE id = 1;", update.Sql);
            update = formatter.Format(statement);
            Assert.Null(update.Sql);

            obj.Name = "bbb";
            obj.PlayTime = 10000;

            update = formatter.Format(statement);
            Assert.Equal("UPDATE Players SET name = 'bbb', play_time = 10000 WHERE id = 1;", update.Sql);
        }


        /*
        [Fact]
        public void TestLoadByLambda()
        {
            var objs = _repository.Load((x) => _t0.Name == "aaa");
            Assert.Single(objs);
            var objs2 = _repository.Load((x) => _t0.Age == 10);
            Assert.Equal(2, objs2.Count());
            Assert.Equal(1, objs2.ElementAt(0).Id);
            Assert.Equal(3, objs2.ElementAt(1).Id);
        }

        [Fact]
        public void TestLoadByLambdaAndVariables()
        {
            var c = "aaa";
            var objs = _repository.Load(x => _t0.Name == c);
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);

            var age = 10;
            var objs2 = _repository.Load(x => _t0.Age == age);
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
            var objs = _repository.Load(x => _t0.Name == c);
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);

            var age = 10;
            var objs2 = _repository.Load(x => _t0.Age == age);
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
            var objs = _repository.Load(x => _t0.Name != "aaa");
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestLoadbyLambdaAnd()
        {
            var objs = _repository.Load(x => _t0.Name == "aaa" && _t0.Age != 10);
            Assert.Empty(objs);
        }

        [Fact]
        public void TestLoadByLambdaWithJoins()
        {
            var objs = _repository.Load(st => st.Where(x => _t0.Age == 10)
                .Joins(x => _t0.Characters, (x, r) => _t0.Id == r.Player.Id)
                .Joins(x => _t0.LastCharacters, (x, r) => _t0.Id == r.LastPlayer.Id));
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
            var objs = _repository.Load(st => st.Where(x => _t0.Age == 10)
                .Joins(x => _t0.Characters, (x, r) => x == r.Player)
                .Joins(x => _t0.LastCharacters, (x, r) => x == r.LastPlayer));
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
            var objs = _repository.Load(st => st.Where(x => _t0.Age == 10)
                .Joins(x => _t0.Characters));
            Assert.Equal(2, objs.Count());
            Assert.Equal(2, objs.ElementAt(0).Characters.Count());
            Assert.Empty(objs.ElementAt(1).Characters);
        }

        [Fact]
        public void TestLoadByLambdaWithJoinsAndHasManyOnPlayer2()
        {
            var objs = _repository.Load(st => st.Where(x => _t0.Age == 10)
                .Joins(x => _t0.Characters)
                .Joins(x => _t0.LastCharacters));
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
            Assert.Equal("ccc's", rset.Results.ElementAt(2)["name"]);
            Assert.Equal("Create Test", rset.Results.ElementAt(3)["name"]);
            Assert.Equal((Int64)999, rset.Results.ElementAt(3)["age"]);
            Assert.Equal("Create Test 2", rset.Results.ElementAt(4)["name"]);
            Assert.Equal((Int64)10000, rset.Results.ElementAt(4)["play_time"]);

            Assert.Equal(4, p.Id);
            Assert.Equal(5, q.Id);
            Assert.Equal(1, r.Id);

            var rset2 = _connection.Execute("SELECT * FROM Characters");
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

            _connection.Execute(sql + string.Join(", ", values));

            var rset = _connection.Execute("SELECT * FROM Characters");
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

            _characterRepository.Store(characters.ToArray());

            var rset = _connection.Execute("SELECT * FROM Characters");
            Assert.Equal(10003, rset.Results.Count());
        }

        [Fact]
        public void TestDelete()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(4, rset2.Results.Count());

            _repository.Delete((x) => _t0.Id == 4);
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
        public void TestDeleteByLamdaAndName()
        {
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });
            _repository.Store(new Player() { Name = "yyy" });
            _repository.Store(new Player() { Name = "zzz" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            _repository.Delete(x => _t0.Name == "yyy");

            var rset3 = _connection.Execute("SELECT * FROM Players");
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
            var rset = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset.Results.Count());

            _repository.Store(new Player() { Name = "xxx" });
            _repository.Store(new Player() { Name = "yyy" });
            _repository.Store(new Player() { Name = "zzz" });

            var rset2 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(6, rset2.Results.Count());

            var names = new[] { "xxx", "yyy", "zzz" };
            _repository.Delete(x => names.Contains(_t0.Name));

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
            _repository.Delete(x => ids.Contains(_t0.Id));

            var rset3 = _connection.Execute("SELECT * FROM Players");
            Assert.Equal(3, rset3.Results.Count());
            Assert.Equal((Int64)1, rset3.Results.ElementAt(0)["id"]);
            Assert.Equal((Int64)2, rset3.Results.ElementAt(1)["id"]);
            Assert.Equal((Int64)3, rset3.Results.ElementAt(2)["id"]);
        }
        */
    }
}