using System;
using System.Linq;

namespace Meuzz.Persistence
{
    [AttributeUsage(AttributeTargets.Class)]
    public class PersistentAttribute : Attribute
    {
        public string TableName = null;
        public string PrimaryKey = null;

        public PersistentAttribute(string tableName) : this(tableName, "id")
        {
        }

        public PersistentAttribute(string TableName, string PrimaryKey = null)
        {
            this.TableName = TableName;
            this.PrimaryKey = PrimaryKey;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name = null;
        public ColumnAttribute(string Name = null)
        {
            this.Name = Name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class BelongsToAttribute : ColumnAttribute
    {
        public BelongsToAttribute(Type Type, string Name = null) : base(Name)
        {
            this.Type = Type;
        }

        public Type Type = null;
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class HasManyAttribute : Attribute
    {
        public string ForeignKey = null;
        public string PrimaryKey = null;
        public Type Through = null;
        public string ThroughForeignKey = null;

        public HasManyAttribute()
        {
        }
    }

    public abstract class PersistableState
    {
        public string[] DirtyKeys { get; }

        public PersistableState(string[] dirtyKeys)
        {
            DirtyKeys = dirtyKeys;
        }

        public static bool IsNew(object obj)
        {
            if (!(obj is IPersistable ip))
            {
                throw new ArgumentException("Non-Persistable object passed");
            }

            lock (ip.__Metadata)
            {
                return ip.__Metadata.IsNew;
            }
        }

        public static PersistableState Get(object obj)
        {
            if (!(obj is IPersistable ip))
            {
                return null;
            }

            lock (ip.__Metadata)
            {
                var state = ip.__Metadata.GetDirtyState();
                return state;
            }
        }

        public static void Reset(object obj)
        {
            if (!(obj is IPersistable ip))
            {
                return;
            }

            lock (ip.__Metadata)
            {
                ip.__Metadata.ResetDirtyState();
                return;
            }
        }
    }

    public interface IPersistableMetadata
    {
        PersistableState GetDirtyState();

        void ResetDirtyState();

        bool IsNew { get; }
    }

    public interface IPersistable
    {
        IPersistableMetadata __Metadata { get; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class BackingFieldAttribute : Attribute
    {
        public BackingFieldAttribute(string backingFieldName)
        {
            BackingFieldName = backingFieldName;
        }

        public string BackingFieldName { get; }
    }
}
