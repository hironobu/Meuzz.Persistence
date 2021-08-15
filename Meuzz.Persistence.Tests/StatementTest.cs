using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Meuzz.Persistence.Sql;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class Unknown
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [HasMany(ForeignKey = "parent_id")]
        public IEnumerable<ChildUnknown> Children { get; set; }
    }

    public class ChildUnknown
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Name { get; set; }

        [HasMany(ForeignKey = "parent_id")]
        public IEnumerable<ChildUnknown2> Children { get; set; }
    }

    public class ChildUnknown2
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Name { get; set; }
    }


    public class StatementTest
    {
        [Fact]
        public void TestSelect()
        {
            var statement = new SelectStatement<Unknown>();
            Assert.Null(statement.Condition);

            statement.Where(x => x.Id == 1);
            Assert.NotNull(statement.Condition);

            Assert.Equal("x => (x.Id == 1)", statement.Condition.ToString());
        }

        [Fact]
        public void TestSelectWithKeyAndValue()
        {
            var statement = new SelectStatement<Unknown>();
            Assert.Null(statement.Condition);

            statement.Where("id", 1);
            Assert.NotNull(statement.Condition);

            Assert.Equal("x => (Convert(x.Id, Int32) == 1)", statement.Condition.ToString());
        }

        [Fact]
        public void TestSelectWithKeyAndValue2()
        {
            var statement = new SelectStatement<Unknown>();
            Assert.Null(statement.Condition);

            statement.Where("id", 1, 2, 3);
            Assert.NotNull(statement.Condition);

            Assert.Equal("x => value(System.Object[]).Contains(Convert(x.Id, Object))", statement.Condition.ToString());
        }

        [Fact]
        public void TestSelectAndJoin()
        {
            var statement = new SelectStatement<Unknown>();
            Assert.Null(statement.Condition);

            statement.Where(x => x.Id == 1);
            Assert.NotNull(statement.Condition);

            Assert.Equal("x => (x.Id == 1)", statement.Condition.ToString());

            var statement2 = statement.Joins(x => x.Children);
        }
    }
}