#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Meuzz.Foundation;
using Meuzz.Persistence.Core;

namespace Meuzz.Persistence
{
    public static class TypeInfoExtensions
    {
        /// <summary>
        ///   テーブル情報を取得する。
        /// </summary>
        /// <param name="t">対象となる型情報。</param>
        /// <returns>テーブル情報。</returns>
        /// <exception cref="NotImplementedException">Persistent化を受けてない型情報が与えられた場合。</exception>
        public static TableInfoManager.Entry? GetTableInfo(this Type t)
        {
            if (!t.IsPersistent())
            {
                throw new NotImplementedException();
            }

            var ti = TableInfoManager.Instance().GetEntry(t);
            if (ti != null)
            {
                return ti;
            }

            return null;
        }

        /// <summary>
        ///   Persistent化を受けた型かどうか。
        /// </summary>
        /// <param name="t">対象となる型情報。</param>
        /// <returns>Persistent化を受けた型ならばtrue。</returns>
        public static bool IsPersistent(this Type t)
        {
            return t.GetCustomAttribute<PersistentAttribute>() != null;
        }

        /// <summary>
        ///   カラム名からプロパティ情報を取得する。
        /// </summary>
        /// <param name="t">プロパティ情報の検索対象となる型。</param>
        /// <param name="columnName">カラム名。</param>
        /// <param name="usingPrimaryKey">
        ///   プロパティの型のプライマリキーを検索に使うか否か。
        ///   trueの場合、対象プロパティ名と対象プロパティ型のプライマリキー名を連結した文字列を<paramref name="columnName"/>の検索対象に含める。
        /// </param>
        /// <returns>検索に一致したプロパティ情報。一致したものがない場合はnull。</returns>
        public static PropertyInfo? GetPropertyInfoFromColumnName(this Type t, string columnName, bool usingPrimaryKey = false)
        {
            var c = GetShortColumnName(columnName).ToLower();
            foreach (var p in t.GetProperties())
            {
                var cc = p.Name.ToSnake().ToLower();
                var ppa = p.GetCustomAttribute<ColumnAttribute>();
                if (ppa != null && ppa.Name != null)
                {
                    cc = ppa.Name.ToLower();
                }

                if (cc == c || (usingPrimaryKey && $"{cc}_{(p.PropertyType.GetPrimaryKey() ?? "id").ToLower()}" == c))
                {
                    return p;
                }
            }

            return null;
        }

        /// <summary>
        ///   対象の型におけるプロパティキーを取得する。
        /// </summary>
        /// <param name="t">対象となる型。</param>
        /// <returns>プロパティキー名。</returns>
        public static string? GetPrimaryKey(this Type t)
        {
            var attr = t.GetCustomAttribute<PersistentAttribute>();
            if (attr != null && attr.PrimaryKey != null)
            {
                return attr.PrimaryKey;
            }
            return null;
        }

        public static object? GetPrimaryValue(this Type t, object obj)
        {
            var pkey = t.GetPrimaryKey();
            if (pkey == null) { return null; }

            var propPKey = t.GetProperty(pkey.ToCamel(true));
            if (propPKey == null) { return null; }
            
            // TODO: defaultの値も(場合によっては)正規の値として処理できる(=nullを返さない)ようにせよ
            var pval = propPKey.GetValue(obj);
            if (pval is int)
            {
                return default(int) != (int)pval ? pval : null;
            }
            if (pval is long)
            {
                return default(long) != (long)pval ? pval : null;
            }

            return pval;
        }

        /// <summary>
        ///   型情報から連動するテーブル名を取得する。
        /// </summary>
        /// <remarks>
        ///   <para>
        ///     <see cref="PersistentAttribute"/>による指定があれば、<see cref="PersistentAttribute.TableName"/>を返す。
        ///   </para>
        ///   <para>
        ///     指定がなければ、型名から<see cref="StringUtilsExtensions.ToSnake"/>した値を自動生成して返す。
        ///   </para>
        /// </remarks>
        /// <param name="t">型情報。</param>
        /// <returns>テーブル名。</returns>
        public static string GetTableName(this Type t)
        {
            var attr = t.GetCustomAttribute<PersistentAttribute>();
            if (attr == null || attr.TableName == null)
            {
                return t.Name.ToSnake();
            }
            return attr.TableName;
        }

        /// <summary>
        ///   対象オブジェクトから型情報とカラム名を指定して値を取り出す。
        /// </summary>
        /// <param name="t">型情報。テーブル化したときにカラムに<paramref name="col"/>を持つものとする。</param>
        /// <param name="col">カラム名。</param>
        /// <param name="obj">対象オブジェクト。</param>
        /// <returns>取得された値。</returns>
        public static object? GetValueForColumnName(this Type t, string col, object obj)
        {
            var propInfo = t.GetPropertyInfoFromColumnName(col);
            return propInfo?.GetValue(obj);
        }

        /// <summary>
        ///   対象オブジェクトから型情報とカラム名の配列を指定して値を取り出し、辞書オブジェクトを生成する。
        /// </summary>
        /// <param name="t">型情報。テーブル化したときにカラムに<paramref name="cols"/>を持つものとする。</param>
        /// <param name="cols">カラム名。</param>
        /// <param name="obj">対象オブジェクト。</param>
        /// <returns>取得された値と<paramref name="cols"/>を組み合わせた辞書オブジェクト。</returns>
        public static IDictionary<string, object?> GetValueDictFromColumnNames(this Type t, string[] cols, object obj)
        {
            return cols.Zip(t.GetValuesFromColumnNames(cols, obj), (x, y) => new { x, y }).ToDictionary(x => x.x, x => x.y);
        }

        /// <summary>
        ///   対象オブジェクトから、型情報とカラム名の配列を指定して値を取り出す。
        /// </summary>
        /// <param name="t">型情報。テーブル化したときにカラムに<paramref name="cols"/>を持つものとする。</param>
        /// <param name="cols">カラム名。</param>
        /// <param name="obj">対象オブジェクト。</param>
        /// <returns>取得された値のコレクション。個数と順序は<paramref name="cols"/>に一致することを保証する。</returns>
        public static IEnumerable<object?> GetValuesFromColumnNames(this Type t, string[] cols, object obj)
        {
            return cols.Select(c => t.GetValueForColumnName(c, obj));
        }

        /// <summary>
        ///   外部キーを取得する。
        /// </summary>
        /// <remarks>
        ///   <para>以下の条件に従ってカラム情報を検索する。</para>
        ///   <list type="bullet">
        ///     <item><paramref name="predictedForeignKey"/>で始まる名前を持つカラム。</item>
        ///     <item><see cref="TableInfoManager.ColumnInfoEntry.BindingToPrimaryKey"/>が<paramref name="primaryKey"/>と一致するかnull。</item>
        ///     <item><see cref="TableInfoManager.ColumnInfoEntry.BindingTo"/>が<paramref name="primaryType"/>の示すテーブル名(<see cref="GetTableName(Type)"/>)と一致する。</item>
        ///   </list>
        /// </remarks>
        /// <param name="t">外部キーを持つ側の型情報。</param>
        /// <param name="predictedForeignKey">予測された外部キー。前方一致で検索する。</param>
        /// <param name="primaryType"><paramref name="primaryKey"/>を持つ側の型情報。</param>
        /// <param name="primaryKey">プライマリキー。</param>
        /// <returns>取得された外部キー。</returns>
        /// <exception cref="NotImplementedException"></exception>
        public static string GetForeignKey(this Type t, string predictedForeignKey, Type primaryType, string primaryKey)
        {
            var ci = t.GetTableInfo();
            if (ci == null) { throw new NotImplementedException(); }
            return ci.Columns.Where(x => x.Name.StartsWith(predictedForeignKey)
                && (x.BindingToPrimaryKey == null || x.BindingToPrimaryKey == primaryKey)
                && (x.BindingTo == null || x.BindingTo == primaryType.GetTableName())).Single().Name;
        }

        private static string GetShortColumnName(string fcol)
        {
            return fcol.Split('.').Last();
        }
    }

    public static class MemberInfoExtensions
    {
        /// <summary>
        ///   メンバー情報から連動するDB上のカラム名を取得する。
        /// </summary>
        /// <remarks>
        ///   <para>
        ///     <see cref="ColumnAttribute"/>による指定があれば、<see cref="ColumnAttribute.Name"/>を返す。
        ///   </para>
        ///   <para>
        ///     指定がなければ、メンバー名から<see cref="StringUtilsExtensions.ToSnake"/>した値を自動生成して返す。
        ///   </para>
        /// </remarks>
        /// <param name="memberInfo">メンバー情報。</param>
        /// <returns>カラム名。</returns>
        public static string GetColumnName(this MemberInfo memberInfo)
        {
            var attr = memberInfo.GetCustomAttribute<ColumnAttribute>();
            if (attr == null || attr.Name == null)
            {
                return memberInfo.Name.ToSnake();
            }

            return attr.Name.ToLower();
        }
    }
}
