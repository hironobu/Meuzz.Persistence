using System;
using System.Collections.Generic;
using System.Text;

namespace Meuzz.Persistence.Tests
{
    [PersistentClass("Players")]
    public class Player
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany(ForeignKey: "player_id")]
        public IEnumerable<Character> Characters { get; set; }

        [HasMany(ForeignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; set; }

        // public IEnumerable<Item> Items { get; set; }
    }

    public class Item
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }
    }


    [PersistentClass("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        // public Geometry Location { get; set; }

        public Player Player { get; set; }

        public Player LastPlayer { get; set; }
    }

    public class Geometry
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }

        public double Altitude { get; set; }
    }
}


namespace Meuzz.Persistence.Tests.Models.NoForeignKeyProperty
{
    [PersistentClass("Players")]
    public class Player
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany(ForeignKey: "player_id")]
        public IEnumerable<Character> Characters { get; set; }

        [HasMany(ForeignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; set; }
    }

    [PersistentClass("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        // public Geometry Location { get; set; }
    }

}

namespace Meuzz.Persistence.Tests.Models.AutoForeignKey
{
    [PersistentClass("Players")]
    public class Player
    {
        public int Id { get; set; }

        [PersistentProperty]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany]
        public IEnumerable<Character> Characters { get; set; }
    }

    [PersistentClass("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public Player Player { get; set; }
    }
}


namespace Meuzz.Persistence.Tests.Models.UsingFields
{
    [PersistentClass("Players")]
    public class Player
    {
        public int Id;

        public string Name;

        public int Age;

        public int PlayTime;

        [HasMany(ForeignKey: "player_id")]
        public IEnumerable<Character> Characters { get; set; }

        [HasMany(ForeignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; set; }
    }


    [PersistentClass("Characters")]
    public class Character
    {
        public int Id;

        public string Name;

        public Player Player;

        public Player LastPlayer;
    }
}