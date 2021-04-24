using System;
using System.Collections.Generic;
using System.Linq;
using Meuzz.Persistence.Sql;
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

        public TreeNode Parent { get; set; }
    }

    public class TreeNodeTest
    {
        private IPersistenceContext _context;
        private ObjectRepository _repository;

        public TreeNodeTest()
        {
            var engine = PersistenceEngineFactory.Instance().GetEngine("sqlite");

            _context = engine.CreateContext("type=sqlite;file=:memory:");
            _context.Connection.Open();

            _context.Connection.Execute(@"
                CREATE TABLE TreeNodes (ID integer PRIMARY KEY, NAME text, PARENT_ID integer);
            ");
            _context.Connection.Execute(@"
                INSERT INTO TreeNodes VALUES (1, 'aa', NULL), (2, 'bbb', 1), (3, 'ccc', 1), (4, 'ddd', 1);
                INSERT INTO TreeNodes VALUES (5, 'aaaa', 2), (6, 'bbbb', 3), (7, 'cccc', 4), (8, 'dddd', 4), (9, 'eeee', 4), (10, 'ffff', 4), (11, 'gggg', 4);
            ");
            _repository = new ObjectRepository(_context.Connection, _context.Formatter, new SqlCollator());
        }

        [Fact]
        public void TestLoadById()
        {
            var objs = _repository.Load<TreeNode>(s => s.Where(x => x.Id == 1)).ToList();
            Assert.Single(objs);
            Assert.Equal(1, objs.ElementAt(0).Id);
            Assert.Equal(3, objs.ElementAt(0).Children.Count());
            Assert.Equal("bbb", objs.ElementAt(0).Children.ElementAt(0).Name);
            Assert.Equal("ccc", objs.ElementAt(0).Children.ElementAt(1).Name);
            Assert.Equal("ddd", objs.ElementAt(0).Children.ElementAt(2).Name);
            Assert.Single(objs.ElementAt(0).Children.ElementAt(0).Children);
            Assert.Single(objs.ElementAt(0).Children.ElementAt(1).Children);
            Assert.Equal(5, objs.ElementAt(0).Children.ElementAt(2).Children.Count());

            Assert.NotNull(objs.ElementAt(0).Children.ElementAt(0).Parent);
            Assert.Equal(1, objs.ElementAt(0).Children.ElementAt(0).Parent.Id);

            var objs2 = _repository.Load<TreeNode>(2);
            Assert.Single(objs2);
            Assert.Equal((Int64)2, objs2.ElementAt(0).Id);
            Assert.Equal("aa", objs2.ElementAt(0).Parent.Name);
            Assert.Equal(1, objs2.ElementAt(0).Parent.Id);

            var objs3 = _repository.Load<TreeNode>(1, 2, 3);
            Assert.Equal(3, objs3.Count());
            Assert.Equal((Int64)1, objs3.ElementAt(0).Id);
            Assert.Equal("aa", objs3.ElementAt(0).Name);
            Assert.Equal((Int64)2, objs3.ElementAt(1).Id);
            Assert.Equal("bbb", objs3.ElementAt(1).Name);
            Assert.Equal((Int64)3, objs3.ElementAt(2).Id);
            Assert.Equal("ccc", objs3.ElementAt(2).Name);
        }
    }
}