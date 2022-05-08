using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Meuzz.Persistence.Tests.Models;

namespace Meuzz.Persistence.Tests
{
    // dummy
    public class PersistableStateImpl : PersistableState
    {
        public PersistableStateImpl(string[] dirtyKeys) : base(dirtyKeys.Where(x => !string.IsNullOrEmpty(x)).ToArray())
        {
        }
    }
}

namespace Meuzz.Persistence.Tests.Models.Sample
{
    public class Player0
    {
        public string Name
        {
            get => _name;
            set 
            {
                if (_name != value)
                {
                    _name = value;
                }
            }
        }

        private string _name = string.Empty;
    }

    public class Player1
    {
        public string Name
        {
            get => _name;
            set
            {
                lock (this)
                {
                    if (_name != value)
                    {
                        _name = value;

                        __dirty["Name"] = true;
                    }
                }
            }
        }

        public PersistableState GetDirtyState()
        {
            PersistableState state;

            lock (this)
            {
                state = new PersistableStateImpl(__dirty.Keys.ToArray());
                __dirty.Clear();
            }
            return state;
        }

        private string _name = string.Empty;

        private IDictionary<string, object> __dirty = new Dictionary<string, object>();
    }

    public class Player2
    {
        public string Name
        {
            get => _name;
            set
            {
                lock (this)
                {
                    if (_name != value)
                    {
                        _name = value;

                        _Name__dirty = true;
                    }
                }
            }
        }

        public int Age
        {
            get => _age;
            set
            {
                lock (this)
                {
                    if (_age != value)
                    {
                        _age = value;

                        _Age__dirty = true;
                    }
                }
            }
        }

        public PersistableState GetDirtyState()
        {
            PersistableState state;

            lock (this)
            {
                state = new PersistableStateImpl(new[]
                {
                    _Name__dirty ? "Name" : null,
                    _Age__dirty ? "Age" : null,
                });
            }

            return state;
        }

        public void ResetDirtyState()
        {
            lock (this)
            {
                _Name__dirty = false;
                _Age__dirty = false;
            }
        }

        private string _name = default(string);
        private int _age = default(int);

        // [IsDirty]
        private bool _Name__dirty = false;
        // [IsDirty]
        private bool _Age__dirty = false;
    }

}

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

        public string _HiddenString { get; set; }

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