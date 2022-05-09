using System.Linq;
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
            var context = PersistableState.Get(c);
            Assert.Empty(context.DirtyKeys);
        }

        [Fact]
        public void TestSingleKeyDirty()
        {
            var c = new Character();
            c.Name = "aaa";
            var context = PersistableState.Get(c);
            PersistableState.Reset(c);
            Assert.Single(context.DirtyKeys);
            Assert.Equal("Name", context.DirtyKeys[0]);

            context = PersistableState.Get(c);
            Assert.Empty(context.DirtyKeys);
        }

        [Fact]
        public void TestSingleKeyDirty2()
        {
            var p = new Player();
            p.Name = "aaa";
            p.Age = 111;
            p.PlayTime = 222;
            var context = PersistableState.Get(p);
            var dirtyKeys = context.DirtyKeys.OrderBy(x => x).ToArray();
            Assert.Equal(3, context.DirtyKeys.Length);
            Assert.Equal("Age", dirtyKeys[0]);
            Assert.Equal("Name", dirtyKeys[1]);
            Assert.Equal("PlayTime", dirtyKeys[2]);
            PersistableState.Reset(p);

            context = PersistableState.Get(p);
            Assert.Empty(context.DirtyKeys);
        }
    }
}
