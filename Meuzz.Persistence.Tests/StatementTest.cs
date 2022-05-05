using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Meuzz.Persistence.Sql;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    [Persistent("Teams")]
    public class Team
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [HasMany(ForeignKey = "parent_id")]
        public IEnumerable<Member> Players { get; set; }
    }

    [Persistent("Members")]
    public class Member
    {
        public int Id { get; set; }

        public int TeamId { get; set; }

        public string Name { get; set; }
    }

    [Persistent("Matches")]
    public class Match
    {
        public int Id { get; set; }

        public int Team1Id { get; set; }
        
        public int Team2Id { get; set; }

        public DateTime MatchDay { get; set; }

        public string Result { get; set; }
    }


    public class StatementTest
    {
        [Fact]
        public void TestSelect()
        {
            // SELECT * FROM Member
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);
            Assert.Empty(statement.RelationSpecs);
            Assert.Null(statement.OutputSpec);
            Assert.NotNull(statement.ParameterSetInfo);
            Assert.Single(statement.ParameterSetInfo.GetAllParameters());
            Assert.Equal("_t", statement.ParameterSetInfo.GetAllParameters().First().Item1);
            Assert.Equal(typeof(Member), statement.ParameterSetInfo.GetAllParameters().First().Item2);
        }

        [Fact]
        public void TestSelectAndWhere()
        {
            // SELECT * FROM Member x WHERE ID = 1
            var statement = new SelectStatement<Member>().Where(x => x.Id == 1);
            Assert.NotNull(statement.Condition);
            Assert.Equal("x => (x.Id == 1)", statement.Condition.ToString());
            Assert.Empty(statement.RelationSpecs);
            Assert.Null(statement.OutputSpec);

            Assert.Single(statement.ParameterSetInfo.GetAllParameters());
            Assert.Equal("x", statement.ParameterSetInfo.GetAllParameters().First().Item1);
            Assert.Equal(typeof(Member), statement.ParameterSetInfo.GetAllParameters().First().Item2);
        }

        [Fact]
        public void TestSelectWithKeyAndValue()
        {
            // SELECT * FROM Member x WHERE ID = 1
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);

            var statement2 = statement.Where("id", 1);
            Assert.NotNull(statement2.Condition);

            Assert.Equal("x => (Convert(x.Id, Int32) == 1)", statement2.Condition.ToString());
        }

        [Fact]
        public void TestSelectWithKeyAndValue2()
        {
            // SELECT * FROM Member x WHERE ID IN (1, 2, 3)
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);

            var statement2 = statement.Where("id", 1, 2, 3);
            Assert.NotNull(statement2.Condition);

            Assert.Equal("x => value(System.Object[]).Contains(Convert(x.Id, Object))", statement2.Condition.ToString());
        }

        [Fact]
        public void TestSelectAndJoin()
        {
            // SELECT * FROM Team t LEFT JOIN Member m ON t.ID = m.TeamID;
            var statement = new SelectStatement<Team>();
            Assert.Null(statement.Condition);

            statement = statement.Where(x => x.Id == 1);
            Assert.NotNull(statement.Condition);

            statement = statement.Joins<Member>(x => x.Players, (x, m) => (x.Id == m.TeamId));

            Assert.Equal("x => (x.Id == 1)", statement.Condition.ToString());
        }

        [Fact]
        public void TestSelectAndSingleColumn()
        {
            // SELECT ID AS Code, NAME AS Title FROM Member WHERE ID = 1;
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);

            var statement2 = statement.Where(x => x.Id == 1);
            Assert.NotNull(statement2.Condition);

            Assert.Equal("x => (x.Id == 1)", statement2.Condition.ToString());

            var statement3 = statement2.Select(x => x.Id);

            Assert.Single(statement3.ColumnSpecs);
            Assert.NotNull(statement3.Condition);
            Assert.Empty(statement3.RelationSpecs);
            Assert.NotNull(statement3.OutputSpec);
            Assert.Null(statement3.Source);

            // Assert.Empty(statement2.ColumnSpecs);
            Assert.NotNull(statement2.Condition);
            Assert.Empty(statement2.RelationSpecs);
            Assert.Null(statement2.OutputSpec);
        }


        [Fact]
        public void TestSelectAndMultipleColumns()
        {
            // SELECT ID AS Code, NAME AS Title FROM Member WHERE ID = 1;
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);

            var statement2 = statement.Where(x => x.Id == 1);
            Assert.NotNull(statement2.Condition);
            Assert.Null(statement.Condition);

            Assert.Equal("x => (x.Id == 1)", statement2.Condition.ToString());

            var statement3 = statement2.Select(x => new { Code = x.Id, Title = x.Name });

            Assert.Equal(2, statement3.ColumnSpecs.Length);
            Assert.NotNull(statement3.Condition);
            Assert.Empty(statement3.RelationSpecs);
            Assert.NotNull(statement3.OutputSpec);
            
            Assert.Null(statement3.Source);

            Assert.Empty(statement2.ColumnSpecs);
            Assert.NotNull(statement2.Condition);
            Assert.Empty(statement2.RelationSpecs);
            Assert.Null(statement2.OutputSpec);
        }

        [Fact]
        public void TestSelectAndOutputOtherTypeWithNotImplementedException()
        {
            // SELECT COUNT AS COUNT(*), MAX AS MAX(TITLE) FROM (SELECT ID AS Code, NAME AS Title FROM Member WHERE ID = 1)
            var statement = new SelectStatement<Member>();
            Assert.Null(statement.Condition);

            var statement2 = statement.Where(x => x.Id == 1);
            Assert.NotNull(statement2.Condition);

            Assert.Equal("x => (x.Id == 1)", statement2.Condition.ToString());

            Assert.Throws<NotImplementedException>(() =>
            {
                var statement3 = statement2.Select(x => new { Code = x.Id, Title = x.Name }).Select((x, i, arr) => new { Count = arr.Count(), Max = arr.Max(x => x.Title) });

                Assert.Null(statement3.Condition);
                Assert.Empty(statement3.RelationSpecs);
                Assert.NotNull(statement3.OutputSpec);
                Assert.Equal("HOGEHOGE", statement3.OutputSpec.OutputExpression.ToString());
            });
        }
    }
}