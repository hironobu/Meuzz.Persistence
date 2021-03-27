using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class TableInfoTest
    {
        [Fact]
        public void Test01()
        {
            var ti = typeof(Player).GetTableInfo();
            Assert.Equal(4, ti.Columns.Length);
            var cis = ti.Columns.OrderBy(c => c.Name);

            Assert.Equal("age", cis.ElementAt(0).Name);
            Assert.Equal("id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("play_time", cis.ElementAt(3).Name);
        }

        [Fact]
        public void Test02()
        {
            var ti = typeof(Character).GetTableInfo();
            Assert.Equal(4, ti.Columns.Length);
            var cis = ti.Columns.OrderBy(c => c.Name);

            Assert.Equal("id", cis.ElementAt(0).Name);
            Assert.Equal("last_player_id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("player_id", cis.ElementAt(3).Name);
        }
    }

    public class TableInfoTestUsingFields
    {
        [Fact]
        public void Test01()
        {
            var ti = typeof(Models.UsingFields.Player).GetTableInfo();
            Assert.Equal(4, ti.Columns.Length);
            var cis = ti.Columns.OrderBy(c => c.Name);

            Assert.Equal("age", cis.ElementAt(0).Name);
            Assert.Equal("id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("play_time", cis.ElementAt(3).Name);
        }

        [Fact]
        public void Test02()
        {
            var ti = typeof(Models.UsingFields.Character).GetTableInfo();
            Assert.Equal(4, ti.Columns.Length);
            var cis = ti.Columns.OrderBy(c => c.Name);

            Assert.Equal("id", cis.ElementAt(0).Name);
            Assert.Equal("last_player_id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("player_id", cis.ElementAt(3).Name);
        }
    }

}
