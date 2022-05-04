using System.IO;
using System.Reflection;
using Microsoft.Build.Utilities;
using Mono.Cecil;

namespace Meuzz.Persistence.Builder
{
    public class ModuleManager
    {
        /// <summary>
        ///   コンストラクター。
        /// </summary>
        /// <param name="references"></param>
        public ModuleManager(string[] references)
        {
            _references = references;
        }

        /// <summary>
        ///   アセンブリファイルを読み込み、モジュール定義情報を生成する。
        /// </summary>
        /// <remarks>
        ///   このメソッドの後、<paramref name="assemblyFileName"/>のファイルはオープンされたままになります。
        /// </remarks>
        /// <param name="assemblyFileName">対象のアセンブリファイル。</param>
        /// <returns>モジュール定義情報。</returns>
        public (ModuleDefinition, bool) ReadModule(string assemblyFileName)
        {
            var baseDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var assemblyResolver = new DefaultAssemblyResolver();
            assemblyResolver.AddSearchDirectory(baseDir);

            foreach (var path in _references)
            {
                var p = path;
                if (File.Exists(p))
                {
                    p = Path.GetDirectoryName(path);
                }
                assemblyResolver.AddSearchDirectory(p);
            }

            var mainModule = ModuleDefinition.ReadModule(assemblyFileName,
                new ReaderParameters
                {
                    ReadWrite = true,
                    AssemblyResolver = assemblyResolver,
                }
            );

            var hasSymbols = false;
            try
            {
                mainModule.ReadSymbols();
                hasSymbols = true;
            }
            catch
            {
                // skip.
            }

            return (mainModule, hasSymbols);
        }

        /// <summary>
        ///   モジュール定義情報を用いてアセンブリファイルを書き出す。
        /// </summary>
        /// <param name="moduleDefinition">モジュール定義情報。</param>
        /// <param name="strongNameKeyPair">書き出し時に署名を行うための鍵ペア。</param>
        /// <param name="publicKey">署名を行うための公開鍵。</param>
        /// <param name="assemblyFileName">書き出し先のファイル名。</param>
        /// <param name="hasSymbols">シンボルを含むかどうか。</param>
        public void WriteModule(ModuleDefinition moduleDefinition, StrongNameKeyPair strongNameKeyPair, byte[] publicKey, bool hasSymbols)
        {
            // for debug and temporary solution
            //var originalAssemblyFileName = $"{assemblyFileName}.orig";
            //File.Delete(originalAssemblyFileName);
            //File.Move(assemblyFileName, originalAssemblyFileName);

            moduleDefinition.Assembly.Name.PublicKey = publicKey;
            moduleDefinition.Write(new WriterParameters
            {
                StrongNameKeyPair = strongNameKeyPair,
                WriteSymbols = hasSymbols
            });
        }

        private string[] _references;
    }
}
