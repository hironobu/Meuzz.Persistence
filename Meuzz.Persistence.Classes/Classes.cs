﻿using System;

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

    public class PersistenceContext
    {
        public string[] DirtyKeys { get; }

        public PersistenceContext(string[] dirtyKeys)
        {
            DirtyKeys = dirtyKeys;
        }

        public static PersistenceContext Generate(object obj)
        {
            if (!(obj is IPersistable ip))
            {
                return null;
            }

            return ip.GeneratePersistenceContext();
        }
    }

    public interface IPersistable
    {
        PersistenceContext GeneratePersistenceContext();
    }
}