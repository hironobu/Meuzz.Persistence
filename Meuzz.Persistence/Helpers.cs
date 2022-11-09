using System;
using System.Collections.Generic;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence
{
    public static class ReflectionHelpers
    {
        public static object? PropertyGet(object? obj, string propertyName)
        {
            return obj?.GetType()?.GetProperty(propertyName.ToCamel(true))?.GetValue(obj);
        }

        public static object? PropertyOrFieldGet(object? obj, MemberInfo mi)
        {
            return mi switch
            {
                FieldInfo fi => fi.GetValue(obj),
                PropertyInfo pi => pi.GetValue(obj),
                _ => throw new NotImplementedException(),
            };
        }

        public static void PropertySet(object? obj, string propertyName, object? value)
        {
            if (obj == null) { return; }
            obj.GetType().GetProperty(propertyName.ToCamel(true), BindingFlags.InvokeMethod)?.SetValue(obj, value);
        }

        [Obsolete]
        public static void PropertyOrFieldSet(object obj, PropertyInfo propInfo, object value)
        {
            if (propInfo.SetMethod != null)
            {
                propInfo.SetValue(obj, value);
            }
            else
            {
                var attr = propInfo.GetCustomAttribute<BackingFieldAttribute>();
                if (attr != null)
                {
                    var field = propInfo.DeclaringType?.GetField(attr.BackingFieldName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                    field?.SetValue(obj, value);
                }
            }
        }

        public static object? DictionaryGet(object? obj, string memberName)
        {
            var dx = obj as IDictionary<string, object?>;
            if (dx == null)
            {
                return null;
            }

            return DictionaryGet(dx, memberName);
        }

        public static object? DictionaryGet(IDictionary<string, object?> dx, string memberName)
        {
            var col = memberName.ToSnake();
            if (dx.ContainsKey(col))
            {
                return dx[col];
            }
            if (col != "id" && dx.ContainsKey(col + "_id"))
            {
                return dx[col + "_id"];
            }

            return null;
        }

#if false
        [Obsolete("DO NOT USE", true)]
        public static object? MemberGet(IDictionary<string, object?> x, string memberName)
        {
            if (x.ContainsKey(memberName))
            {
                return x[memberName];
            }
            return ReflectionHelpers.PropertyGet(x["__object"], memberName);
        }
#endif
    }
}