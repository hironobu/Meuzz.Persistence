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

        public static void PropertySet(object? obj, string propertyName, object? value)
        {
            if (obj == null) { return; }
            obj.GetType().GetProperty(propertyName.ToCamel(true), BindingFlags.InvokeMethod)?.SetValue(obj, value);
        }

        public static object? DictionaryGet(object? obj, string memberName)
        {
            var dx = obj as IDictionary<string, object?>;
            if (dx == null)
            {
                return null;
            }

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
    }
}