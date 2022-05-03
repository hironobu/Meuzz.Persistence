using System;

namespace Meuzz.Persistence
{
    [AttributeUsage(AttributeTargets.Class)]
    public class PersistentClassAttribute : Attribute
    {
        public string TableName = null;
        public string PrimaryKey = null;

        public PersistentClassAttribute(string tableName) : this(tableName, "id")
        {
        }

        public PersistentClassAttribute(string tableName, string primaryKey = null)
        {
            this.TableName = tableName;
            this.PrimaryKey = primaryKey;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class PersistentPropertyAttribute : Attribute
    {
        public string Column = null;
        public PersistentPropertyAttribute(string column = null)
        {
            Column = column;
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

    public class PersistableState
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

            return ip.GeneratePersistableState();
        }
    }

    public interface IPersistable
    {
        PersistableState GeneratePersistableState();
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
