using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Mono.Cecil;
using Mono.Cecil.Cil;
using Mono.Collections.Generic;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using Meuzz.Persistence;
using System.Collections.Generic;
using System.Collections;

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

        public void Run()
        {
            var moduleManager = new ModuleManager(References.Split(';'));

            var assemblyFileName = AssemblyFile;
            var (mainModule, hasSymbols) = moduleManager.ReadModule(assemblyFileName);

            var types = mainModule.GetTypes().Where(t => t.HasCustomAttributes && t.CustomAttributes.Any(ca => ca.AttributeType.FullName == typeof(PersistentClassAttribute).FullName));
            foreach (var t in types)
            {
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


        private static MethodReference MakeHostInstanceGeneric(MethodReference self, params TypeReference[] genericArguments)
        {
            GenericInstanceType genericDeclaringType = new GenericInstanceType(self.DeclaringType);
            foreach (TypeReference genericArgument in genericArguments)
            {
                genericDeclaringType.GenericArguments.Add(genericArgument);
            }

            MethodReference reference = new MethodReference(self.Name, self.ReturnType, genericDeclaringType)
            {
                HasThis = self.HasThis,
                ExplicitThis = self.ExplicitThis,
                CallingConvention = self.CallingConvention
            };

            foreach (ParameterDefinition parameter in self.Parameters)
            {
                reference.Parameters.Add(new ParameterDefinition(parameter.ParameterType));
            }

            foreach (GenericParameter genericParam in self.GenericParameters)
            {
                reference.GenericParameters.Add(new GenericParameter(genericParam.Name, reference));
            }

            return reference;
        }

        private void AddPropertyGetterLoader(ModuleDefinition module, TypeDefinition td, PropertyDefinition pd, Instruction[] originalSetterInstructions)
        {
            try
            {
                var ptr = pd.PropertyType;
                var loadertype = MakeGenericType(module.ImportReference(typeof(IEnumerable<>)), ptr);
                var loadername = $"__load_{pd.Name}";
                var loader = new FieldDefinition(loadername, Mono.Cecil.FieldAttributes.Private, loadertype);
                if (td.Fields.Any(x => x.Name == loadername))
                {
                    return;
                }

                td.Fields.Add(loader);

                var enumerable = module.ImportReference(typeof(Enumerable)).Resolve();
                var singleOrDefault = module.ImportReference(enumerable.Methods.Single(x => x.Name == "SingleOrDefault" && x.Parameters.Count == 1));
                // var singleOrDefault = loader.FieldType.Resolve().Methods.Single(x => x.Name == "SingleOrDefault");
                singleOrDefault = MakeGenericMethod(singleOrDefault, pd.PropertyType);

                var getter = pd.GetMethod;
                var ilp = getter.Body.GetILProcessor();
                var first = getter.Body.Instructions[0];
                var ret = getter.Body.Instructions.Last();

                var setter = pd.SetMethod;
                // var setterInstructions = setter.Body.Instructions;

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
                // ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, pr.SetMethod));
                foreach (var si in originalSetterInstructions.Skip(2))
                {
                    if (si.OpCode == OpCodes.Ret) { break; }

                    ilp.InsertBefore(ret, si);
                }
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));

                ilp.InsertBefore(ret, il_0038);
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_2));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Br_S, il_003c));

                ilp.InsertBefore(ret, il_003c);
            }
            catch (Exception ex)
            {
                Log.LogError(ex.Message);
                Log.LogError(ex.StackTrace);
                throw ex;
            }
        }

        private void AddPropertyDirtySetter(ModuleDefinition module, TypeDefinition td, PropertyDefinition pd, FieldDefinition dirtyDict, Instruction[] originalGetterInstructions)
        {
            var dictionary = module.ImportReference(typeof(IDictionary<,>)).Resolve();
            var setItem = module.ImportReference(MakeHostInstanceGeneric(dictionary.Methods.Single(x => x.Name == "set_Item" && x.Parameters.Count == 2), module.ImportReference(typeof(string)), module.ImportReference(typeof(bool))));

            var setter = pd.SetMethod;
            var ilp = setter.Body.GetILProcessor();
            var first = setter.Body.Instructions[0];
            var ret = setter.Body.Instructions.Last();

            var getter = pd.GetMethod;
            var getterInstructions = getter.Body.Instructions;

            setter.Body.Variables.Add(new VariableDefinition(module.ImportReference(typeof(bool))));

            var il_002c = ret;

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));
            foreach (var i in originalGetterInstructions)
            {
                if (i.OpCode == OpCodes.Ret) { break; }

                ilp.InsertBefore(first, i);
            }
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldarg_1));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ceq));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldc_I4_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ceq));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stloc_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldloc_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Brfalse_S, il_002c));

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));

            // original code here

            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, dirtyDict));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldstr, pd.Name));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldc_I4_1));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Callvirt, setItem));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
        }

        private void WeaveTypeAsPersistent(ModuleDefinition module, TypeDefinition td)
        {
            var dirtyDictName = "__dirty";

            if (td.Fields.Any(x => x.Name == dirtyDictName))
            {
                return;
            }

            var ipersistable = module.ImportReference(typeof(IPersistable));
            td.Interfaces.Add(new InterfaceImplementation(ipersistable));
            
            var dirtydict = module.ImportReference(MakeGenericType(module.ImportReference(typeof(IDictionary<,>)), module.ImportReference(typeof(string)), module.ImportReference(typeof(bool))));
            var dirtydictfield = new FieldDefinition(dirtyDictName, Mono.Cecil.FieldAttributes.Private, dirtydict);
            td.Fields.Add(dirtydictfield);

            var dirtydictInstance = module.ImportReference(typeof(Dictionary<,>)).Resolve();
            var dirtydictInstanceCtor = module.ImportReference(MakeHostInstanceGeneric(dirtydictInstance.Methods.Single(x => x.Name == ".ctor" && x.Parameters.Count == 0), module.ImportReference(typeof(string)), module.ImportReference(typeof(bool))));
            var ctor = td.Methods.Single(x => x.Name == ".ctor");
            var ilp = ctor.Body.GetILProcessor();
            var first = ctor.Body.Instructions[0];

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldarg_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Newobj, dirtydictInstanceCtor));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stfld, dirtydictfield));

            var getKeys = module.ImportReference(MakeHostInstanceGeneric(module.ImportReference(typeof(IDictionary<,>)).Resolve().Methods.Single(x => x.Name == "get_Keys"), module.ImportReference(typeof(string)), module.ImportReference(typeof(bool))));
            var toArray = module.ImportReference(MakeGenericMethod(module.ImportReference(typeof(Enumerable)).Resolve().Methods.Single(x => x.Name == nameof(Enumerable.ToArray)), module.ImportReference(typeof(string))));

            var generatePersistableState = new MethodDefinition(nameof(IPersistable.GeneratePersistableState), Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Virtual | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName, module.ImportReference(typeof(PersistableState)));
            var ilpGeneratePersistableState = generatePersistableState.Body.GetILProcessor();

            generatePersistableState.Body.Variables.Add(new VariableDefinition(module.ImportReference(typeof(string[]))));
            generatePersistableState.Body.Variables.Add(new VariableDefinition(module.ImportReference(typeof(PersistableState))));

            var persistenceContextCtor = module.ImportReference(module.ImportReference(typeof(PersistableState)).Resolve().Methods.Single(x => x.Name == ".ctor"));
            var clear = module.ImportReference(module.ImportReference(typeof(IDictionary)).Resolve().Methods.Single(x => x.Name == nameof(IDictionary.Clear)));

            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ldfld, dirtydictfield));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Callvirt, getKeys));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Call, toArray));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Stloc_0));

            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ldfld, dirtydictfield));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Callvirt, clear));

            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Nop));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ldloc_0));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Newobj, persistenceContextCtor));
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Stloc_1));

            var il_0027 = Instruction.Create(OpCodes.Ldloc_1);
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Br_S, il_0027));

            ilpGeneratePersistableState.Append(il_0027);
            ilpGeneratePersistableState.Append(Instruction.Create(OpCodes.Ret));

            td.Methods.Add(generatePersistableState);

            // dirty flag operations
            var props = td.Properties;
            foreach (var pr in props)
            {
                var ptr = pr.PropertyType;
                if (pr.SetMethod == null)
                {
                    continue;
                }

                var originalGetterInstructions = pr.GetMethod.Body.Instructions.ToArray();
                var originalSetterInstructions = pr.SetMethod.Body.Instructions.ToArray();

                if (!ptr.IsGenericInstance && !ptr.FullName.StartsWith("System.") && originalSetterInstructions != null)
                {
                    AddPropertyGetterLoader(module, td, pr, originalSetterInstructions);
                }

                AddPropertyDirtySetter(module, td, pr, dirtydictfield, originalGetterInstructions);
            }
        }
    }
}
