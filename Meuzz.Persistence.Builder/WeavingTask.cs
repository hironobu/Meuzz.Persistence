using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Mono.Cecil;
using Mono.Cecil.Cil;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Meuzz.Persistence;
using System.Collections.Generic;

namespace Meuzz.Persistence.Builder
{
    public class WeavingTask : Task, ICancelableTask
    {
        public string AssemblyFile { get; set; } = null;

        public string IntermediateDirectory { get; set; } = null;

        public ITaskItem[] PackageReferences { get; set; } = null;

        public string ProjectDirectory { get; set; } = null;

        public string ProjectFile { get; set; } = null;

        public string References { get; set; } = null;

        public string SolutionDirectory { get; set; } = null;

        public string KeyOriginatorFile { get; set; }
        public string AssemblyOriginatorKeyFile { get; set; }

        public bool SignAssembly { get; set; }
        public bool DelaySign { get; set; }

        public void Cancel()
        {
            throw new NotImplementedException();
        }

        public override bool Execute()
        {
            Log.LogMessage(MessageImportance.High, $"AssemblyFile: {AssemblyFile}");
            Log.LogMessage(MessageImportance.High, $"IntermediateDirectory: {IntermediateDirectory}");
            Log.LogMessage(MessageImportance.High, $"PackageReferences: {PackageReferences}");
            Log.LogMessage(MessageImportance.High, $"ProjectDirectory: {ProjectDirectory}");
            Log.LogMessage(MessageImportance.High, $"ProjectFile: {ProjectFile}");
            Log.LogMessage(MessageImportance.High, $"References: {References}");
            Log.LogMessage(MessageImportance.High, $"SolutionDirectory: {SolutionDirectory}");
            Log.LogMessage(MessageImportance.High, $"KeyOriginatorFile: {KeyOriginatorFile}");
            Log.LogMessage(MessageImportance.High, $"AssemblyOriginatorKeyFile: {AssemblyOriginatorKeyFile}");
            Log.LogMessage(MessageImportance.High, $"SignAssembly: {SignAssembly}");
            Log.LogMessage(MessageImportance.High, $"DelaySign: {DelaySign}");

            Run();

            return true;
        }

        public bool IsType(TypeReference tr, Type t)
        {
            if (tr.FullName != t.FullName)
            {
                Log.LogMessage(MessageImportance.High, $"tr: [{tr.FullName}], t.FullName: {t.FullName}");
                return false;
            }

            var td = tr.Resolve();
            var t0 = Type.GetType(td.FullName);
            Log.LogMessage(MessageImportance.High, $"t0: [{t0}], td.FullName: {td.FullName}, td.Module.Name: {td.Module.Name}");
            return t == t0;
        }


        public void Run()
        {
            var moduleManager = new ModuleManager(References.Split(';'));

            var assemblyFileName = AssemblyFile;
            var (mainModule, hasSymbols) = moduleManager.ReadModule(assemblyFileName);

            var types = mainModule.GetTypes().Where(t => t.HasCustomAttributes && t.CustomAttributes.Any(ca => ca.AttributeType.FullName == typeof(PersistentClassAttribute).FullName));
            foreach (var t in types)
            {
                Log.LogMessage(MessageImportance.High, $"Type: {t}");

                WeaveTypeAsPersistent(mainModule, t);
            }

            StrongNameKeyPair strongNameKeyPair = null;
            byte[] publicKey = null;
            if (SignAssembly)
            {
                var keySign = new KeySign();
                var keyFilePath = keySign.GetKeyFilePath(mainModule, IntermediateDirectory, KeyOriginatorFile ?? AssemblyOriginatorKeyFile);
                (strongNameKeyPair, publicKey) = keySign.LoadStrongNameKeyEntry(keyFilePath, DelaySign);
            }

            moduleManager.WriteModule(mainModule, strongNameKeyPair, publicKey, assemblyFileName, hasSymbols);
        }

        private static TypeReference MakeGenericType(TypeReference type, params TypeReference[] arguments)
        {
            if (type.GenericParameters.Count != arguments.Length)
                throw new ArgumentException();

            var instance = new GenericInstanceType(type);
            foreach (var argument in arguments)
            {
                instance.GenericArguments.Add(argument);
            }

            return instance;
        }

        private static MethodReference MakeGenericMethod(MethodReference self, params TypeReference[] arguments)
        {
            if (self.GenericParameters.Count != arguments.Length)
                throw new ArgumentException();

            var instance = new GenericInstanceMethod(self);
            foreach (var argument in arguments)
                instance.GenericArguments.Add(argument);

            return instance;
        }

        private void WeaveTypeAsPersistent(ModuleDefinition module, TypeDefinition td)
        {
            // here
            var props = td.Properties;
            foreach (var pr in props)
            {
                var ptr = pr.PropertyType;
                if (ptr.IsGenericInstance || ptr.FullName.StartsWith("System."))
                {
                    var t = module.ImportReference(ptr);
                    Log.LogMessage(MessageImportance.High, $"t: {t} skipped");
                    continue;
                }

                try
                {
                    var loadertype = MakeGenericType(module.ImportReference(typeof(IEnumerable<>)), ptr);
                    var loadername = $"__load_{pr.Name}";
                    var loader = new FieldDefinition(loadername, Mono.Cecil.FieldAttributes.Private, loadertype);
                    if (td.Fields.Any(x => x.Name == loadername)) { continue; }

                    td.Fields.Add(loader);

                    var enumerable = module.ImportReference(typeof(System.Linq.Enumerable)).Resolve();
                    var singleOrDefault = module.ImportReference(enumerable.Methods.Single(x => x.Name == "SingleOrDefault" && x.Parameters.Count == 1));
                    // var singleOrDefault = loader.FieldType.Resolve().Methods.Single(x => x.Name == "SingleOrDefault");
                    singleOrDefault = MakeGenericMethod(singleOrDefault, pr.PropertyType);
                    Log.LogMessage(MessageImportance.High, $"singleOrDefault: [{singleOrDefault.GetType()}]{singleOrDefault}");

                    var pd = ptr.Resolve();
                    Log.LogMessage(MessageImportance.High, $"pd: {pd.Name} {pd.FullName} {pd.Module} {pd.DeclaringType}");

                    var getter = pr.GetMethod;
                    var ilp = getter.Body.GetILProcessor();
                    var first = getter.Body.Instructions[0];
                    var ret = getter.Body.Instructions.Last();

                    getter.Body.Variables.Add(new VariableDefinition(ptr));
                    getter.Body.Variables.Add(new VariableDefinition(module.ImportReference(typeof(bool))));
                    getter.Body.Variables.Add(new VariableDefinition(ptr));
                    getter.Body.Variables.Add(new VariableDefinition(module.ImportReference(typeof(bool))));

                    var il_0015 = Instruction.Create(OpCodes.Ldarg_0);
                    var il_0038 = Instruction.Create(OpCodes.Ldloc_0);
                    var il_003c = Instruction.Create(OpCodes.Ldloc_2);

                    ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));

                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldnull));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Cgt_Un));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_1));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_1));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Brfalse_S, il_0015));
                    
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_2));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Br_S, il_003c));

                    ilp.InsertBefore(ret, il_0015);
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, loader));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldnull));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Cgt_Un));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_3));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_3));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Brfalse_S, il_0038));

                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, loader));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, singleOrDefault));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_0));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, pr.SetMethod));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));

                    ilp.InsertBefore(ret, il_0038);
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_2));
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Br_S, il_003c));

                    ilp.InsertBefore(ret, il_003c);
                }
                catch (Exception ex)
                {
                    Log.LogMessage(MessageImportance.High, $"{ex.Message}");
                    Log.LogMessage(MessageImportance.High, $"{ex.StackTrace}");
                    throw ex;
                }
            }
        }
    }
}
