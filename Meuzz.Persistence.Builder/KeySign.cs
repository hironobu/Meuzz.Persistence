using Mono.Cecil;
using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Meuzz.Persistence.Builder
{
    public class KeySign
    {

        public (StrongNameKeyPair StrongNameKeyPair, byte[] PublicKey) LoadStrongNameKeyEntry(string keyFilePath, bool delaySign)
        {
            if (keyFilePath == null)
            {
                return (null, null);
            }

            if (!File.Exists(keyFilePath))
            {
                throw new Exception($"key file not found: '{keyFilePath}'.");
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


        public string GetKeyFilePath(ModuleDefinition moduleDefinition, string intermediateDirectoryPath, string keyFilePath)
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
            if (assemblyKeyFileAttribute != null)
            {
                var keyFileSuffix = (string)assemblyKeyFileAttribute.ConstructorArguments.First().Value;
                return Path.Combine(intermediateDirectoryPath, keyFileSuffix);
            }
            return null;
        }
    }
}
