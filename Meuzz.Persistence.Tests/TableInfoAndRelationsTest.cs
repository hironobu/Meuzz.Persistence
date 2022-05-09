using System.Linq;
using Meuzz.Persistence.Tests.Models;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class TableInfoAndRelationsTest
    {
        [Fact]
        public void Test01()
        {
            var ci = typeof(Player).GetTableInfo();
            Assert.NotNull(ci);
            Assert.Equal(typeof(Player), ci.Type);
            Assert.Equal(2, ci.Relations.Length);

            var ris = ci.Relations.OrderBy(x => x.ForeignKey);
            Assert.Equal("last_player_id", ris.ElementAt(0).ForeignKey);
            Assert.Equal(typeof(Character), ris.ElementAt(0).TargetType);
            Assert.Equal(typeof(Player).GetProperty("LastCharacters"), ris.ElementAt(0).PropertyInfo);
            Assert.Equal(typeof(Character).GetProperty("LastPlayer"), ris.ElementAt(0).InversePropertyInfo);
            Assert.Equal("player_id", ris.ElementAt(1).ForeignKey);
            Assert.Equal(typeof(Character), ris.ElementAt(1).TargetType);
            Assert.Equal(typeof(Player).GetProperty("Characters"), ris.ElementAt(1).PropertyInfo);
            Assert.Equal(typeof(Character).GetProperty("Player"), ris.ElementAt(1).InversePropertyInfo);
        }

        [Fact]
        public void TestNoForeignKeyProperty()
        {
            var ci = typeof(Models.NoForeignKeyProperty.Player).GetTableInfo();
            Assert.NotNull(ci);
            Assert.Equal(typeof(Models.NoForeignKeyProperty.Player), ci.Type);
            Assert.Equal(2, ci.Relations.Length);

            var ris = ci.Relations.OrderBy(x => x.ForeignKey);
            Assert.Equal("last_player_id", ris.ElementAt(0).ForeignKey);
            Assert.Equal(typeof(Models.NoForeignKeyProperty.Character), ris.ElementAt(0).TargetType);
            Assert.Equal(typeof(Models.NoForeignKeyProperty.Player).GetProperty("LastCharacters"), ris.ElementAt(0).PropertyInfo);
            Assert.Null(ris.ElementAt(0).InversePropertyInfo);
            Assert.Equal("player_id", ris.ElementAt(1).ForeignKey);
            Assert.Equal(typeof(Models.NoForeignKeyProperty.Character), ris.ElementAt(1).TargetType);
            Assert.Equal(typeof(Models.NoForeignKeyProperty.Player).GetProperty("Characters"), ris.ElementAt(1).PropertyInfo);
            Assert.Null(ris.ElementAt(1).InversePropertyInfo);
        }

        [Fact]
        public void TestAutoForeignKey()
        {
            var ci = typeof(Models.AutoForeignKey.Player).GetTableInfo();
            Assert.NotNull(ci);
            Assert.Equal(typeof(Models.AutoForeignKey.Player), ci.Type);
            Assert.Single(ci.Relations);

            var ris = ci.Relations.OrderBy(x => x.ForeignKey);
            Assert.Equal("player_id", ris.ElementAt(0).ForeignKey);
            Assert.Equal(typeof(Models.AutoForeignKey.Character), ris.ElementAt(0).TargetType);
            Assert.Equal(typeof(Models.AutoForeignKey.Player).GetProperty("Characters"), ris.ElementAt(0).PropertyInfo);
            Assert.Equal(typeof(Models.AutoForeignKey.Character).GetProperty("Player"), ris.ElementAt(0).InversePropertyInfo);
        }
    }
}
