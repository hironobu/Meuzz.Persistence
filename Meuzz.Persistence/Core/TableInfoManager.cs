using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Core
{
    /// <summary>
    ///   テーブル情報。
    /// </summary>
    public class TableInfo
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        /// <param name="type">型情報。</param>
        /// <param name="columnInfos">カラム情報。</param>
        /// <param name="relationInfos">リレーション情報。</param>
        public TableInfo(Type type, ColumnInfo[] columnInfos, RelationInfo[] relationInfos)
        {
            Type = type;
            Columns = columnInfos;
            Relations = relationInfos;
        }

        /// <summary>
        ///   型情報。
        /// </summary>
        public Type Type { get; }

        /// <summary>
        ///   カラム情報。
        /// </summary>
        public ColumnInfo[] Columns { get; }

        /// <summary>
        ///   リレーション情報。
        /// </summary>
        public RelationInfo[] Relations { get; }
    }

    /// <summary>
    ///   リレーション情報。
    /// </summary>
    public class RelationInfo
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        /// <param name="propertyInfo">リレーションを示すプロパティ情報。</param>
        /// <param name="targetType">リレーションを受ける子テーブル側の型情報。</param>
        /// <param name="inversePropertyInfo">リレーションを示す逆参照側のプロパティ情報。</param>
        /// <param name="foreignKey">外部キー。</param>
        /// <param name="primaryKey">親テーブルのプライマリキー。</param>
        public RelationInfo(PropertyInfo propertyInfo, Type targetType, PropertyInfo? inversePropertyInfo, string foreignKey, string? primaryKey = null, Type? throughType = null, string? throughForeignKey = null)
        {
            PropertyInfo = propertyInfo;
            TargetType = targetType;
            InversePropertyInfo = inversePropertyInfo;
            ForeignKey = foreignKey;
            PrimaryKey = primaryKey;
            ThroughType = throughType;
            ThroughForeignKey = throughForeignKey;
        }

        /// <summary>
        ///   リレーションを示すプロパティ情報。
        /// </summary>
        public PropertyInfo PropertyInfo { get; }

        /// <summary>
        ///   リレーションを受ける子テーブル側の型情報。
        /// </summary>
        public Type TargetType { get; }

        /// <summary>
        ///   リレーションを示す逆参照側のプロパティ情報。
        /// </summary>
        /// <remarks>
        ///   <see cref="HasManyAttribute"/>による外部キー制約の明示がない場合、この逆参照側のプロパティを用いて自動生成する。
        /// </remarks>
        public PropertyInfo? InversePropertyInfo { get; }

        /// <summary>
        ///   外部キー。
        /// </summary>
        public string ForeignKey { get; }

        /// <summary>
        ///   親テーブルのプライマリキー。
        /// </summary>
        public string? PrimaryKey { get; }

        public Type? ThroughType { get; }

        public string? ThroughForeignKey { get; }
    }

    /// <summary>
    ///   カラム情報。
    /// </summary>
    public class ColumnInfo
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        /// <param name="name">カラム名。</param>
        /// <param name="memberInfo"><paramref name="name"/>に紐づけられたメンバー情報。省略可。</param>
        /// <param name="foreignKeyInfo">外部キー情報。省略可。</param>
        public ColumnInfo(string name, MemberInfo? memberInfo = null, ForeignKeyInfo? foreignKeyInfo = null)
        {
            Name = name;
            MemberInfo = memberInfo;
            BindingTo = foreignKeyInfo?.PrimaryTableName;
            BindingToPrimaryKey = foreignKeyInfo?.PrimaryKey;
        }

        /// <summary>
        ///   カラム名。
        /// </summary>
        public string Name { get; }

        /// <summary>
        ///   メンバー情報。
        /// </summary>
        public MemberInfo? MemberInfo { get; }

        public string? BindingTo { get; }
        public string? BindingToPrimaryKey { get; }
    }

    /// <summary>
    ///   テーブル情報(<see cref="TableInfo"/>)を管理するクラス。
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     プログラムの起動直後に、コード内にあるすべての(Persistable化された)型について紐づけられたテーブル情報を生成して保持する。
    ///   </para>
    ///   <para>
    ///     TODO: 将来的に<see cref="ForeignKeyInfoManager"/>との統合を目指す。
    ///   </para>
    /// </remarks>
    public class TableInfoManager
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        public TableInfoManager()
        {
            _dict = new ConcurrentDictionary<Type, TableInfo>();
        }

        /// <summary>
        ///   テーブル情報を取得する。
        /// </summary>
        /// <remarks>
        ///   <paramref name="type"/>に対する1回目の呼び出しで生成されたテーブル情報は<paramref name="type"/>をキーとしたディクショナリーに登録し
        ///   2回目以降は生成済みのものを返す。
        /// </remarks>
        /// <param name="type">対象となる型情報。</param>
        /// <returns>テーブル情報。</returns>
        public TableInfo GetTableInfo(Type type)
        {
            if (!_dict.TryGetValue(type, out var ti))
            {
                ti = SetupType(type);
            }

            return ti;
        }

        /// <summary>
        ///   テーブル情報を生成する。
        /// </summary>
        /// <param name="type">対象の型。</param>
        /// <remarks>
        ///   <para>
        ///     <paramref name="type"/>に属する各プロパティについて以下の処理を繰り返す。
        ///   </para>
        ///   <list type="number">
        ///     <item>
        ///       <term>当該プロパティに対して外部キー情報が設定されている</term>
        ///       <description>
        ///         <list>
        ///           <term>
        ///             外部キー情報が空でない(<see cref="ForeignKeyInfo.ForeignKey"/>が<c>null</c>以外)
        ///           </term>
        ///           <description>リレーション情報を生成する。</description>
        ///         </list>
        ///         外部キー情報が空ならば何もしない。
        ///       </description>
        ///     </item>
        ///     <item>
        ///       <term>外部キー情報の設定がない</term>
        ///       <description>カラム情報を生成する。</description>
        ///     </item>
        ///   </list>
        ///   <para>
        ///     さらに<paramref name="type"/>の指定するテーブルが他のテーブルを親とした外部キー制約を持っていた場合、該当する外部キーに基づくカラム情報を登録する。
        ///   </para>
        /// </remarks>
        /// <returns>テーブル情報。</returns>
        private TableInfo SetupType(Type type)
        {
            var colinfos = new List<ColumnInfo>();
            var relinfos = new List<RelationInfo>();

            Func<PropertyInfo, bool> condition = x => !x.Name.StartsWith("_");
            foreach (var prop in type.GetProperties().Where(condition))
            {
                var fki = ForeignKeyInfoManager.Instance().GetRelatedForeignKeyInfoByReferencingPropertyInfo(prop);

                if (fki != null)
                {
                    if (fki.ForeignKey != null)
                    {
                        var targetType = prop.PropertyType.IsGenericType ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;
                        var targetInversePropertyInfo = targetType.GetPropertyInfoFromColumnName(fki.ForeignKey, true);

                        var hasmany = prop.GetCustomAttribute<HasManyAttribute>();

                        relinfos.Add(new RelationInfo(prop, targetType, targetInversePropertyInfo, fki.ForeignKey, null, hasmany?.Through, hasmany?.ThroughForeignKey));
                    }
                }
                else
                {
                    colinfos.Add(new ColumnInfo(prop.Name.ToSnake(), prop, fki));
                }
            }

            var fkeys = ForeignKeyInfoManager.Instance().GetForeignKeysByTargetType(type);
            foreach (var fk in fkeys.Where(x => !colinfos.Select(c => c.Name).Contains(x.ForeignKey)))
            {
                colinfos.Add(new ColumnInfo(fk.ForeignKey.ToSnake()));
            }

            var ti = new TableInfo(type, colinfos.ToArray(), relinfos.ToArray());
            _dict.TryAdd(type, ti);

            return ti;
        }

        private ConcurrentDictionary<Type, TableInfo> _dict;

        #region Singleton
        public static TableInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        _instance = new TableInfoManager();
                    }
                }
            }

            return _instance;
        }

        private static TableInfoManager? _instance = null;
        private static readonly object _instanceLocker = new object();
        #endregion
    }
}
