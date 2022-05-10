using System;
using System.Collections.Generic;
using System.Linq;
using Meuzz.Persistence.Database;
using Xunit;

namespace Meuzz.Persistence.Tests.Sqlite
{
    [Persistent("Prefectures")]
    public class Prefecture
    {
        public Prefecture(int id, string name)
        {
            Id = id;
            Name = name;
        }

        public int Id { get; }

        public string Name { get; }

        [HasMany(ForeignKey = "from_id")]
        public IEnumerable<PrefectureEdge> Edges { get; }
#if false
        {
            get
            {
                throw new NotImplementedException();
                // var edges = PrefectureEdge.Objects.Load(context, s => s.Where(x => x.FromId == Id));
                // 
            }
        }
#endif
    }

    [Persistent("PrefectureEdges")]
    public class PrefectureEdge
    {
        public PrefectureEdge(int id, int fromId, int toId)
        {
            Id = id;
            FromId = fromId;
            ToId = toId;
        }

        public int Id { get; }

        public int FromId { get; }

        public int ToId { get; }
    }

    public class PrefecturesTest
    {
        public PrefecturesTest()
        {
            var engine = DatabaseEngineFactory.Instance().CreateEngine("sqlite", "Data Source=:memory:");

            _context = engine.CreateContext();
            _context.Open();

            _context.Execute(@"
                CREATE TABLE Prefectures (ID integer PRIMARY KEY, NAME text);
                CREATE TABLE PrefectureEdges (ID integer PRIMARY KEY, FROM_ID integer, TO_ID integer, FOREIGN KEY (FROM_ID) REFERENCES Prefectures(ID), FOREIGN KEY (TO_ID) REFERENCES Prefectures(ID));
            ");
            _context.Execute(@"
                INSERT INTO Prefectures VALUES (1, 'ñkäCìπ');
                INSERT INTO Prefectures VALUES (2, 'ê¬êX');
                INSERT INTO Prefectures VALUES (3, 'ä‚éË');
                INSERT INTO Prefectures VALUES (4, 'ã{èÈ');
                INSERT INTO Prefectures VALUES (5, 'èHìc');
                INSERT INTO Prefectures VALUES (6, 'éRå`');
                INSERT INTO Prefectures VALUES (7, 'ïüìá');
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (1, 2);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (2, 3);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (2, 5);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (3, 4);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (4, 5);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (4, 6);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (4, 7);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (5, 6);
                INSERT INTO PrefectureEdges (FROM_ID, TO_ID) VALUES (6, 7);
            ");

            _repository = new Repository<Prefecture>();
            _edgeRepository = new Repository<PrefectureEdge>();
        }

        [Fact]
        public void TestLoadById()
        {
            var objs = _repository.Load<Prefecture>(_context, 1);
            Assert.Single(objs);
            Assert.Equal((Int64)1, objs.ElementAt(0).Id);
            var objs2 = _repository.Load<Prefecture>(_context, 2);
            Assert.Single(objs2);
            Assert.Equal((Int64)2, objs2.ElementAt(0).Id);

            var objs3 = _repository.Load<Prefecture>(_context, 1, 2, 3);
            Assert.Equal(3, objs3.Count());
            Assert.Equal((Int64)1, objs3.ElementAt(0).Id);
            Assert.Equal("ñkäCìπ", objs3.ElementAt(0).Name);
            Assert.Equal((Int64)2, objs3.ElementAt(1).Id);
            Assert.Equal("ê¬êX", objs3.ElementAt(1).Name);
            Assert.Equal((Int64)3, objs3.ElementAt(2).Id);
            Assert.Equal("ä‚éË", objs3.ElementAt(2).Name);
        }

        [Fact]
        public void TestLoadEdgeById()
        {
            var objs = _repository.Load<PrefectureEdge>(_context, 1);
            Assert.Single(objs);
            Assert.Equal((Int64)1, objs.ElementAt(0).FromId);
            Assert.Equal((Int64)2, objs.ElementAt(0).ToId);
        }


        [Fact]
        public void TestLoadByIdAndEdges()
        {
            var prefs = _repository.Load<Prefecture>(_context, 4);
            Assert.Single(prefs);
            Assert.Equal("ã{èÈ", prefs.First().Name);

            var edges = prefs.First().Edges.ToList();
            Assert.Equal(3, edges.Count());
        }


        [Fact]
        public void TestLoadByIdAndEdgesWithJoins()
        {
            var edges = _edgeRepository.Load(_context, st => st.Where(x => x.FromId == 4).Joins<Prefecture>((x, y) => x.ToId == y.Id));
            Assert.Single(edges);
            Assert.Equal(4, edges.First().Item1.FromId);

            //var edges = prefs.First().Edges.ToList();
            //Assert.Equal(3, edges.Count());
        }

#if false
        [Fact]
        public void TestLoadByLambda()
        {
            var objs = _repository.Load<Player>(_context, (x) => x.Name == "red");
            Assert.Single(objs);
            var objs2 = _repository.Load<Player>(_context, (x) => x.Age == 10);
            Assert.Equal(2, objs2.Count());
            Assert.Equal(1, objs2.ElementAt(0).Id);
            Assert.Equal(3, objs2.ElementAt(1).Id);
        }

        [Fact]
        public void TestLoadByLambdaAndVariables()
        {
            var c = "red";
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
            var c = "red";
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
            var objs = _repository.Load<Player>(_context, x => x.Name != "red");
            Assert.Equal(2, objs.Count());
        }

        [Fact]
        public void TestLoadbyLambdaAnd()
        {
            var objs = _repository.Load<Player>(_context, x => x.Name == "red" && x.Age != 10);
            Assert.Empty(objs);
        }

        [Fact]
        public void TestLoadByLambdaWithJoins()
        {
            var objs = _repository.Load<Player>(_context, st => st.Where(x => x.Age == 10)
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

        [Fact]
        public void TestCreateAndUpdate()
        {
            var p = new Player() { Name = "Create Test", Age = 999 };
            var q = new Player() { Name = "Create Test 2", PlayTime = 10000 };
            var r = new Player() { Id = 1 };
            PersistableState.Reset(r);
            r.Name = "Update Test";

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
            Assert.Equal("blue", rset.Results.ElementAt(1)["name"]);
            Assert.Equal("green's", rset.Results.ElementAt(2)["name"]);
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
        private IDatabaseContext _context;

        private Repository<Prefecture> _repository;
        private Repository<PrefectureEdge> _edgeRepository;
    }
}