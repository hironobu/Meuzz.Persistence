using System;
using System.Collections.Generic;
using System.Text;
using Meuzz.Persistence.Tests.Models;

namespace Meuzz.Persistence.Tests.Models
{
    [Persistent("Players")]
    public class Player
    {
        public int Id { get; set; }

        [Column]
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


    [Persistent("Characters")]
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

    public class Geometry2
    {
        public double Latitude { get => _latitude; set => _latitude = value; }
        public double Longitude { get => _longitude; set => _longitude = value; }

        public double Altitude { get => _altitude; set => _altitude = value; }

        private double _latitude;
        private double _longitude;
        private double _altitude;
    }

}

namespace Meuzz.Persistence.Tests.Models.ReadOnly
{
    [Persistent("Players")]
    public class Player
    {
        public int Id { get; }

        [Column]
        public string Name { get; }

        public int Age { get; }

        public int PlayTime { get; }

        [HasMany(ForeignKey: "player_id")]
        public IEnumerable<Character> Characters { get; }

        [HasMany(ForeignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; }

        // public IEnumerable<Item> Items { get; set; }

        public Player(int id, string name, int age, int playTime)
        {
            Id = id;
            Name = name;
            Age = age;
            PlayTime = playTime;
        }
    }

    public class Item
    {
        public int Id { get; }

        public string Name { get; }

        public string Description { get; }

        public Item(int id, string name, string description)
        {
            Id = id;
            Name = name;
            Description = description;
        }
    }


    [Persistent("Characters")]
    public class Character
    {
        public int Id { get; }

        public string Name { get; }

        public Player Player { get; }

        public Player LastPlayer { get; }

        public Character(int id, string name)
        {
            Id = id;
            Name = name;
        }
    }

    public class Geometry
    {
        public double Latitude { get; }
        public double Longitude { get; }

        public double Altitude { get; }

        public Geometry(double latitude, double longitude, double altitude)
        {
            Latitude = latitude;
            Longitude = longitude;
            Altitude = altitude;
        }
    }
}

namespace Meuzz.Persistence.Tests.Models.NoForeignKeyProperty
{
    [Persistent("Players")]
    public class Player
    {
        public int Id { get; set; }

        [Column]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany(ForeignKey: "player_id")]
        public IEnumerable<Character> Characters { get; set; }

        [HasMany(ForeignKey: "last_player_id")]
        public IEnumerable<Character> LastCharacters { get; set; }
    }

    [Persistent("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        // public Geometry Location { get; set; }
    }

}

namespace Meuzz.Persistence.Tests.Models.AutoForeignKey
{
    [Persistent("Players")]
    public class Player
    {
        public int Id { get; set; }

        [Column]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PlayTime { get; set; }

        [HasMany]
        public IEnumerable<Character> Characters { get; set; }
    }

    [Persistent("Characters")]
    public class Character
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public Player Player { get; set; }
    }
}


namespace Meuzz.Persistence.Tests.Models.UsingFields
{
    [Persistent("Players")]
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


    [Persistent("Characters")]
    public class Character
    {
        public int Id;

        public string Name;

        public Player Player;

        public Player LastPlayer;
    }
}