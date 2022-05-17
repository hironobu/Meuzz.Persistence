#nullable enable

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Meuzz.Foundation;

namespace Meuzz.Persistence.Core
{
    /// <summary>
    ///   外部キー情報。
    /// </summary>
    public class ForeignKeyInfo
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        /// <param name="foreignKey">外部キー。</param>
        /// <param name="foreignTableName">外部キーを持つテーブル名。</param>
        /// <param name="primaryKey">親テーブルのプライマリキー。</param>
        /// <param name="primaryTableName">親テーブル名。</param>
        public ForeignKeyInfo(string? foreignKey, string? foreignTableName, string primaryKey, string primaryTableName)
        {
            ForeignKey = foreignKey;
            ForeignTableName = foreignTableName;
            PrimaryKey = primaryKey;
            PrimaryTableName = primaryTableName;
        }

        /// <summary>
        ///   外部キー。
        /// </summary>
        public string? ForeignKey { get; }

        /// <summary>
        ///   外部キーを持つテーブル名。
        /// </summary>
        public string? ForeignTableName { get; }

        /// <summary>
        ///   親テーブルのプライマリキー。
        /// </summary>
        public string PrimaryKey { get; }

        /// <summary>
        ///   親テーブル名。
        /// </summary>
        public string PrimaryTableName { get; }
    }

    /// <summary>
    ///   外部キー情報(<see cref="ForeignKeyInfo"/>)を管理するクラス。
    /// </summary>
    /// <remarks>
    ///   TODO: 将来的に<see cref="TableInfoManager"/>との統合を目指す。
    /// </remarks>
    public class ForeignKeyInfoManager
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        public ForeignKeyInfoManager()
        {
        }

        /// <summary>
        ///   全てのPersistable化された型について、外部キー情報を読み込む。
        /// </summary>
        /// <remarks>
        ///   <para>
        ///     <list type="bullet">
        ///       <item><see cref="PersistentAttribute"/>属性を付与されたクラスに属する</item>
        ///       <item><see cref="HasManyAttribute"/>属性を付与されたプロパティ</item>
        ///     </list>
        ///     上記を満たすプロパティ全件について、下記の処理を適用する。
        ///   </para>
        ///   <list type="bullet">
        ///     <item>
        ///       プロパティに付与された<see cref="HasManyAttribute"/>から<see cref="HasManyAttribute.ForeignKey"/>を取得する
        ///       <list type="bullet">
        ///         <item>もし<see cref="HasManyAttribute.ForeignKey"/>が<c>null</c>ならば、一対多における「多」側の型から逆参照となるプロパティを特定し、当該プロパティ名から外部キー名を自動生成する。</item>
        ///       </list>
        ///     </item>
        ///     <item>
        ///       前項までで取得された外部キー名を、「「多」側の型をキーとするディクショナリ」「「一」側の型が持つプロパティ情報をキーとするディクショナリ」それぞれに登録する。
        ///     </item>
        ///   </list>
        /// </remarks>
        /// <exception cref="NotImplementedException"></exception>
        public void Initialize()
        {
            var typeToForeignKeysTable = new Dictionary<Type, IDictionary<string, bool>>();
            var propertyInfoToForeignKeyTable = new Dictionary<PropertyInfo, string>();

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var type in assembly.GetTypes().Where(t => t.IsDefined(typeof(PersistentAttribute), true)))
                {
                    foreach (var prop in type.GetProperties().Where(p => p.IsDefined(typeof(BelongsToAttribute), true)))
                    {
                        var belongsToAttribute = prop.GetCustomAttributes<BelongsToAttribute>().First();

                        var fk = prop.Name.ToSnake();

                        if (typeToForeignKeysTable.ContainsKey(type))
                        {
                            typeToForeignKeysTable[type][fk] = true;
                        }
                        else
                        {
                            typeToForeignKeysTable.Add(type, new Dictionary<string, bool>(){ { fk, true } });
                        }
                    }
                }

                foreach (var type in assembly.GetTypes().Where(t => t.IsDefined(typeof(PersistentAttribute), true)))
                {
                    var hasManyProps = type.GetProperties().Where(p => p.IsDefined(typeof(HasManyAttribute), true));
                    foreach (var prop in hasManyProps)
                    {
                        var hasmany = prop.GetCustomAttribute<HasManyAttribute>();
                        if (hasmany == null) { throw new NotImplementedException(); }

                        Type targetType;
                        string fk;

                        if (hasmany.Through != null)
                        {
                            continue;
                        }
                        else
                        {
                            targetType = typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) ? prop.PropertyType.GetGenericArguments()[0] : prop.PropertyType;

                            fk = hasmany.ForeignKey;
                        }

                        if (fk == null)
                        {
                            if (hasManyProps.Where(p => p.PropertyType == prop.PropertyType).Count() > 1)
                            {
                                throw new NotImplementedException();
                            }

                            var revprop = targetType.GetProperties().Where(x => x.PropertyType == prop.DeclaringType).Single();
                            fk = revprop.Name.ToSnake() + "_id";
                        }

                        propertyInfoToForeignKeyTable.Add(prop, fk);

                        if (typeToForeignKeysTable.ContainsKey(targetType))
                        {
                            typeToForeignKeysTable[targetType][fk] = true;
                        }
                        else
                        {
                            typeToForeignKeysTable.Add(targetType, new Dictionary<string, bool>() { { fk, true } });
                        }
                    }
                }
            }

            _typeToForeignKeysTable = typeToForeignKeysTable;
            _propertyInfoToForeignKeyTable = propertyInfoToForeignKeyTable;
        }

        public string[] GetForeignKeysByTargetType(Type targetType)
        {
            return _typeToForeignKeysTable?.ContainsKey(targetType) == true ? _typeToForeignKeysTable[targetType].Keys.ToArray() : Array.Empty<string>();
        }

        private string? GetForeignKeyByPropertyInfo(PropertyInfo pi)
        {
            return _propertyInfoToForeignKeyTable?.ContainsKey(pi) == true ? _propertyInfoToForeignKeyTable[pi] : null;
        }

        /// <summary>
        ///   対象のプロパティ情報に対する外部キー情報を取得する。
        /// </summary>
        /// <remarks>
        ///   <para>
        ///     次の条件にしたがって外部キー情報を生成する。
        ///   </para>
        ///   <list type="bullet">
        ///     <item>
        ///       <term>(一対多関係における)逆参照を示すプロパティの場合</term>
        ///       <description>
        ///         <list type="bullet">
        ///           <item>
        ///             <term>Persistent化されていない型の場合</term>
        ///             <description><c>null</c>を返す</description>
        ///           </item>
        ///           <item>
        ///             <term>上記以外</term>
        ///             <description>空の外部キー情報(<c>ForeignKey = null</c>)を返す。</description>
        ///           </item>
        ///         </list>
        ///       </description>
        ///     </item>
        ///     <item>
        ///       <term><see cref="HasManyAttribute"/>を持たないプロパティの場合</term>
        ///       <description>例外射出(<see cref="NotImplementedException"/>)</description>
        ///     </item>
        ///     <item>
        ///       <term>上記以外</term>
        ///       <desciprion>外部キー、外部キーを持つテーブル名、プライマリキー、プライマリキーを持つテーブル名を構成要素とする外部キー情報(<see cref="ForeignKeyInfo"/>)を生成して返す。</desciprion>
        ///     </item>
        ///   </list>
        /// </remarks>
        /// <param name="propertyInfo">対象となるプロパティ情報。</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public ForeignKeyInfo? GetRelatedForeignKeyInfoByReferencingPropertyInfo(PropertyInfo propertyInfo)
        {
            var pt = propertyInfo.PropertyType;

            // (一対多関係における)逆参照を示すプロパティの場合
            if (!(typeof(IEnumerable).IsAssignableFrom(pt) && !typeof(string).IsAssignableFrom(pt)))
            {
                // nullもしくは空の外部キー情報を返す。
                if (!pt.IsPersistent())
                {
                    return null;
                }
                else
                {
                    return GetInversedForeignKeyInfo(pt, propertyInfo);
                }
            }

            // 以下、一対多関係を示すプロパティのみ処理を進める
            var hasmany = propertyInfo.GetCustomAttribute<HasManyAttribute>();
            if (hasmany == null) { throw new NotImplementedException(); }
            var declaringType = propertyInfo.DeclaringType;
            if (declaringType == null) { throw new NotImplementedException(); }
            var primaryKey = hasmany.PrimaryKey ?? declaringType.GetPrimaryKey();
            if (primaryKey == null) { throw new NotImplementedException(); }

            return new ForeignKeyInfo(
                hasmany.ForeignKey ?? (_propertyInfoToForeignKeyTable.ContainsKey(propertyInfo) ? _propertyInfoToForeignKeyTable[propertyInfo] : null),
                propertyInfo.PropertyType.GetTableName(),
                primaryKey,
                declaringType.GetTableName());
        }

        // TODO: 現時点でダミー関数。将来的に廃止するかも？
        private ForeignKeyInfo GetInversedForeignKeyInfo(Type t, PropertyInfo pi)
        {
            return new ForeignKeyInfo(null, null, string.Empty, string.Empty);
        }

        private IDictionary<Type, IDictionary<string, bool>> _typeToForeignKeysTable = default!;
        private IDictionary<PropertyInfo, string> _propertyInfoToForeignKeyTable = default!;

        public static ForeignKeyInfoManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLocker)
                {
                    if (_instance == null)
                    {
                        var instance = new ForeignKeyInfoManager();
                        instance.Initialize();

                        _instance = instance;
                    }
                }
            }

            return _instance;
        }

        private static ForeignKeyInfoManager? _instance = null;
        private static readonly object _instanceLocker = new object();
    }
}
