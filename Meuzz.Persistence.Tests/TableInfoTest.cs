﻿using System;
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
            var ci = typeof(Player).GetClassInfo();
            Assert.Equal(4, ci.Columns.Length);
            var cis = ci.Columns.OrderBy(c => c.Name);

            Assert.Equal("age", cis.ElementAt(0).Name);
            Assert.Equal("id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("play_time", cis.ElementAt(3).Name);
        }

        [Fact]
        public void Test02()
        {
            var ci = typeof(Character).GetClassInfo();
            Assert.Equal(4, ci.Columns.Length);
            var cis = ci.Columns.OrderBy(c => c.Name);

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
            var ci = typeof(Models.UsingFields.Player).GetClassInfo();
            Assert.Equal(4, ci.Columns.Length);
            var cis = ci.Columns.OrderBy(c => c.Name);

            Assert.Equal("age", cis.ElementAt(0).Name);
            Assert.Equal("id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("play_time", cis.ElementAt(3).Name);
        }

        [Fact]
        public void Test02()
        {
            var ci = typeof(Models.UsingFields.Character).GetClassInfo();
            Assert.Equal(4, ci.Columns.Length);
            var cis = ci.Columns.OrderBy(c => c.Name);

            Assert.Equal("id", cis.ElementAt(0).Name);
            Assert.Equal("last_player_id", cis.ElementAt(1).Name);
            Assert.Equal("name", cis.ElementAt(2).Name);
            Assert.Equal("player_id", cis.ElementAt(3).Name);
        }
    }

}
