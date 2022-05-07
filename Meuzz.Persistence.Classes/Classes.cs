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
    public class HasManyAttribute : Attribute
    {
        public string ForeignKey = null;
        public string PrimaryKey = null;

        public HasManyAttribute(string ForeignKey = null, string PrimaryKey = null)
        {
            this.ForeignKey = ForeignKey;
            this.PrimaryKey = PrimaryKey;
        }
    }

    public abstract class PersistableState
    {
        public string[] DirtyKeys { get; }

        public PersistableState(string[] dirtyKeys)
        {
            DirtyKeys = dirtyKeys;
        }

        public static PersistableState Generate(object obj)
        {
            if (!(obj is IPersistable ip))
            {
                return null;
            }

            return ip.GetDirtyState();
        }
    }

    public interface IPersistable
    {
        PersistableState GetDirtyState();
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
