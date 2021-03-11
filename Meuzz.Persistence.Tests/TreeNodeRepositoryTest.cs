using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    [PersistentClass("TreeNodes")]
    public class TreeNode
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        [HasMany(ForeignKey: "parent_id")]
        public IEnumerable<TreeNode> Children { get; set; }
    }

    public class TreeNodeRepositoryTest
    {
        private Connection _connection;
        private ObjectRepository<TreeNode> _repository;

        public TreeNodeRepositoryTest()
        {
            _connection = new SqliteConnectionImpl("dummy.sqlite");
            _connection.Open();

            _connection.Execute(@"
                CREATE TABLE TreeNodes (ID integer PRIMARY KEY, NAME text, PARENT_ID integer);
            ");
            _connection.Execute(@"
                INSERT INTO TreeNodes VALUES (1, 'aaa', NULL), (2, 'bbb', 1), (3, 'ccc', 1), (4, 'ddd', 1);
            ");
            _repository = new ObjectRepository<TreeNode>(_connection, new SqliteSqlBuilder<TreeNode>(), new SqliteFormatter(), new SqliteCollator());
        }

        [Fact]
        public void TestLoadById()
        {
            var objs = _repository.Load(s => s.Where(x => x.Id == 1));
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(0).Children.Count());

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

            IEnumerable<TreeNode> nodes = null;

            nodes = GetList(new string[] { "hogehoge", "fugafuga" });

            foreach (var n in nodes)
            {
                var nn = n;
                Console.WriteLine(nn);
            }
        }

        private IEnumerable<TreeNode> GetList(IEnumerable<string> sources)
        {
            yield return new TreeNode() { Name = sources.ElementAt(0) };
            yield return new TreeNode() { Name = sources.ElementAt(1) };
            yield break;
        }
    }
}