using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Meuzz.Persistence.Tests.Models;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class DirtyKeysTest
    {
        [Fact]
        public void TestDefault()
        {
            var c = new Character();
            var context = PersistableState.Generate(c);
            Assert.Empty(context.DirtyKeys);
        }

        [Fact]
        public void TestSingleKeyDirty()
        {
            var c = new Character();
            c.Name = "aaa";
            var context = PersistableState.Generate(c);
            Assert.Single(context.DirtyKeys);
            Assert.Equal("Name", context.DirtyKeys[0]);

            context = PersistableState.Generate(c);
            Assert.Empty(context.DirtyKeys);
        }

        [Fact]
        public void TestSingleKeyDirty2()
        {
            var p = new Player();
            p.Name = "aaa";
            p.Age = 111;
            p.PlayTime = 222;
            var context = PersistableState.Generate(p);
            var dirtyKeys = context.DirtyKeys.OrderBy(x => x).ToArray();
            Assert.Equal(3, context.DirtyKeys.Length);
            Assert.Equal("Age", dirtyKeys[0]);
            Assert.Equal("Name", dirtyKeys[1]);
            Assert.Equal("PlayTime", dirtyKeys[2]);

            context = PersistableState.Generate(p);
            Assert.Empty(context.DirtyKeys);
        }
    }
}
