using System;
using System.Linq;
using System.Text;

namespace Meuzz.Persistence
{
    public class StringUtils
    {
        public static string ToSnake(string s, bool toupper = false)
        {
            if (s == null) throw new ArgumentNullException("s");

            var ret = new StringBuilder();
            for (int i = 0; i < s.Length; i++)
            {
                var c = s[i];
                if (i > 0 && 'A' <= c && c <= 'Z' && s[i - 1] != '_')
                {
                    ret.Append('_');
                }

                ret.Append(toupper ? Char.ToUpper(c) : Char.ToLower(c));
            }
            return ret.ToString();
        }

        public static string ToCamel(string s, bool upperCamel = false)
        {
            if (s == null) throw new ArgumentNullException("s");

            var ss = s.ToLower().Split('_');
            if (ss.Length == 1)
            {
                return !upperCamel ? s : Capitalize(s);
            }
            return upperCamel
                ? string.Join("", ss.Select(x => Capitalize(x)))
                : string.Join("", ss.Select((x, i) => i == 0 && !upperCamel ? x.ToLower() : Capitalize(x)));
        }

        public static string Capitalize(string s)
        {
            if (s == null) throw new ArgumentNullException("s");

            if (s.Length == 0) { return ""; }
            return Char.ToUpper(s[0]) + s.Substring(1);
        }
    }
}
