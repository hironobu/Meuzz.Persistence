using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Mono.Cecil;
using Mono.Cecil.Cil;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Meuzz.Persistence;

namespace Meuzz.Persistence.Builder
{
    public class ModuleManager
    {
        public string[] References;

        public ModuleManager(string[] references)
        {
            References = references;
        }



        public (ModuleDefinition module, bool hasSymbols) ReadModule(string assemblyFileName)
        {
            var baseDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var assemblyResolver = new DefaultAssemblyResolver();
            assemblyResolver.AddSearchDirectory(baseDir);

            foreach (var path in References)
            {
                var p = path;
                if (File.Exists(p))
                {
                    p = Path.GetDirectoryName(path);
                }
                assemblyResolver.AddSearchDirectory(p);
            }
            
            // for debug and temporary solution
            var originalAssemblyFileName = $"{assemblyFileName}.orig";
            File.Delete(originalAssemblyFileName);
            File.Move(assemblyFileName, originalAssemblyFileName);

            var mainModule = ModuleDefinition.ReadModule(originalAssemblyFileName,
                new ReaderParameters
                {
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
            }

            return (mainModule, hasSymbols);
        }

        public void WriteModule(ModuleDefinition moduleDefinition, StrongNameKeyPair strongNameKeyPair, byte[] publicKey, string assemblyFileName, bool hasSymbols)
        {
            moduleDefinition.Assembly.Name.PublicKey = publicKey;
            moduleDefinition.Write(assemblyFileName, new WriterParameters
            {
                StrongNameKeyPair = strongNameKeyPair,
                WriteSymbols = hasSymbols
            });
        }
    }
}
