using Mono.Cecil;
using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence.Builder
{
    public static class KeySign
    {
        /// <summary>
        ///   鍵情報を読み込む。
        /// </summary>
        /// <param name="keyFilePath">鍵ファイルパス。</param>
        /// <param name="delaySign">遅延読み込みを行うか否か。行う場合はtrue。</param>
        /// <returns>
        ///   鍵ペア情報と公開鍵の内容(バイナリ)。<paramref name="keyFilePath"/>がnullならどちらもnullを返す。
        ///   <paramref name="delaySign"/>がfalseなら<paramref name="keyFilePath"/>を読み込んだ内容だけを返し、鍵ペア情報はnullとなる。
        /// </returns>
        /// <exception cref="FileNotFoundException">指定されたファイルが存在しない。</exception>
        public static (StrongNameKeyPair StrongNameKeyPair, byte[] PublicKey) LoadStrongNameKeyEntry(string keyFilePath, bool delaySign)
        {
            if (keyFilePath == null)
            {
                return (null, null);
            }

            if (!File.Exists(keyFilePath))
            {
                throw new FileNotFoundException($"key file not found: '{keyFilePath}'.");
            }

            var fileBytes = File.ReadAllBytes(keyFilePath);

            if (!delaySign)
            {
                try
                {
                    var strongNameKeyPair = new StrongNameKeyPair(fileBytes);
                    var publicKey = strongNameKeyPair.PublicKey;
                    return (strongNameKeyPair, publicKey);
                }
                catch (ArgumentException)
                {
                    // skip
                }
            }

            return (null, fileBytes);
        }

        /// <summary>
        ///   鍵ファイルのパスを生成する。
        /// </summary>
        /// <param name="moduleDefinition">モジュール定義情報。</param>
        /// <param name="intermediateDirectoryPath">中間ディレクトリパス。(obj)</param>
        /// <param name="keyFilePath">鍵ファイルのパス。</param>
        /// <returns>生成された鍵ファイルパス。<paramref name="keyFilePath"/>がnullでかつモジュール定義情報から<see cref="AssemblyKeyFileAttribute"/>が見つからなかった場合はnull。</returns>
        public static string GetKeyFilePath(ModuleDefinition moduleDefinition, string intermediateDirectoryPath, string keyFilePath)
        {
            if (keyFilePath != null)
            {
                keyFilePath = Path.GetFullPath(keyFilePath);
                return keyFilePath;
            }

            var assemblyKeyFileAttribute = moduleDefinition
                .Assembly
                .CustomAttributes
                .FirstOrDefault(x => x.AttributeType.Name == "AssemblyKeyFileAttribute");
            if (assemblyKeyFileAttribute == null)
            {
                return null;
            }

            var keyFileSuffix = (string)assemblyKeyFileAttribute.ConstructorArguments.First().Value;
            return Path.Combine(intermediateDirectoryPath, keyFileSuffix);
        }
    }
}
