using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Mono.Cecil;
using Mono.Cecil.Cil;

namespace Meuzz.Persistence.Builder
{
    public class WeavingTask : Task, ICancelableTask
    {
        /// <summary>
        ///   対象となるアセンブリファイルパス。
        /// </summary>
        public string AssemblyFile { get; set; } = null;

        /// <summary>
        ///   ビルドにおける中間ディレクトリ。
        /// </summary>
        public string IntermediateDirectory { get; set; } = null;

        public ITaskItem[] PackageReferences { get; set; } = null;

        /// <summary>
        ///   プロジェクトディレクトリ。
        /// </summary>
        public string ProjectDirectory { get; set; } = null;

        /// <summary>
        ///   プロジェクトファイル。
        /// </summary>
        public string ProjectFile { get; set; } = null;

        /// <summary>
        ///   参照先アセンブリ(';'区切り)。
        /// </summary>
        public string References { get; set; } = null;

        /// <summary>
        ///   ソリューションディレクトリ。
        /// </summary>
        public string SolutionDirectory { get; set; } = null;

        /// <summary>
        ///   鍵ファイル名。
        /// </summary>
        public string KeyOriginatorFile { get; set; }

        /// <summary>
        ///   アセンブリへの署名用の鍵ファイル。
        /// </summary>
        public string AssemblyOriginatorKeyFile { get; set; }

        /// <summary>
        ///   アセンブリへの署名をするか。
        /// </summary>
        public bool SignAssembly { get; set; }

        public bool DelaySign { get; set; }

        /// <summary>
        ///   タスクのキャンセルを行うエントリーポイント。
        /// </summary>
        /// <remarks>現時点では未実装のため、<see cref="NotImplementedException"/>を送出するのみである。</remarks>
        /// <exception cref="NotImplementedException">未実装のため。</exception>
        public void Cancel()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///   タスクの開始を行うエントリーポイント。
        /// </summary>
        /// <returns>処理が正常に完了すればtrue。</returns>
        public override bool Execute()
        {
#if DEBUG
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
#endif

            Run();

            return true;
        }

        /// <summary>
        ///   型情報の永続化適応のための処理(weave)を行う。
        /// </summary>
        public void Run()
        {
            var moduleManager = new ModuleManager(References.Split(';'));
            var assemblyFileName = AssemblyFile;

            var (mainModuleDef, hasSymbols) = moduleManager.ReadModule(assemblyFileName);

            using (mainModuleDef)
            {
                var typeDefs = mainModuleDef.GetTypes().Where(t => t.HasCustomAttributes && t.CustomAttributes.Any(ca => ca.AttributeType.FullName == typeof(PersistentAttribute).FullName));
                foreach (var typeDef in typeDefs)
                {
                    WeaveTypeAsPersistent(mainModuleDef, typeDef);
                }

                StrongNameKeyPair strongNameKeyPair = null;
                byte[] publicKey = null;
                if (SignAssembly)
                {
                    var keyFilePath = KeySign.GetKeyFilePath(mainModuleDef, IntermediateDirectory, KeyOriginatorFile ?? AssemblyOriginatorKeyFile);
                    (strongNameKeyPair, publicKey) = KeySign.LoadStrongNameKeyEntry(keyFilePath, DelaySign);
                }

                moduleManager.WriteModule(mainModuleDef, strongNameKeyPair, publicKey, hasSymbols);
            }
        }

        /// <summary>
        ///   対象の型に永続化のための処理(weave)を行う。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        private void WeaveTypeAsPersistent(ModuleDefinition moduleDef, TypeDefinition typeDef)
        {
            // AddGeneratePersistentState(moduleDef, typeDef, dirtyDictFieldDef);

            // add IPersistable interface
            var ipersistableTypeRef = moduleDef.ImportReference(typeof(IPersistable));
            typeDef.Interfaces.Add(new InterfaceImplementation(ipersistableTypeRef));

            // backing field
            var hasManyAttributeTypeRef = moduleDef.ImportReference(typeof(HasManyAttribute));

            foreach (var prop in typeDef.Properties)
            {
                var ptr = prop.PropertyType;
                if (prop.CustomAttributes.Any(x => x.AttributeType.Name == hasManyAttributeTypeRef.Name))
                {
                    AddPropertyBackingFieldAttribute(moduleDef, prop);
                }
            }

            var (fieldTypeDef, isNewFieldDef) = MakeMetadataType(moduleDef, typeDef);

            var metadataFieldDef = new FieldDefinition("<__Metadata>k__BackingField", Mono.Cecil.FieldAttributes.Private, fieldTypeDef);
            var dirtyFieldDefAndNames = new List<(FieldDefinition, string)>();

            // weave properties (for dirty flag operations)
            foreach (var pr in typeDef.Properties)
            {
                var ptr = pr.PropertyType;

                var originalGetterInstructions = pr.GetMethod.Body.Instructions.ToArray();
                var originalSetterInstructions = pr.SetMethod != null ? pr.SetMethod.Body.Instructions.ToArray() : null;

                if (!ptr.IsGenericInstance && !ptr.FullName.StartsWith("System."))
                {
                    AddPropertyGetterLoader(moduleDef, typeDef, pr, originalSetterInstructions);
                }

                if (pr.SetMethod != null)
                {
                    var dirtyFieldDef = MakeDirtyField(moduleDef, typeDef, pr.Name);

                    AddPropertyDirtySetter(moduleDef, typeDef, pr, metadataFieldDef, dirtyFieldDef, originalGetterInstructions);

                    dirtyFieldDefAndNames.Add((dirtyFieldDef, pr.Name));
                }
            }

            BuildMetadataProperty(moduleDef, typeDef, fieldTypeDef, metadataFieldDef, dirtyFieldDefAndNames.Select(x => x.Item1), isNewFieldDef);

            AddGetDirtyState(moduleDef, typeDef, fieldTypeDef, metadataFieldDef, dirtyFieldDefAndNames);
            AddResetDirtyState(moduleDef, typeDef, fieldTypeDef, metadataFieldDef, dirtyFieldDefAndNames, isNewFieldDef);
            AddPropertyIsNew(moduleDef, fieldTypeDef, isNewFieldDef);
        }

        /// <summary>
        ///   対象のプロパティに対して、lazyloadで動作するgetterメソッドを設定する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="propDef">対象となるプロパティ定義情報。</param>
        /// <param name="originalSetterInstructions">当該プロパティにおける変更前のsetterメソッドIL。</param>
        private void AddPropertyGetterLoader(ModuleDefinition moduleDef, TypeDefinition typeDef, PropertyDefinition propDef, Instruction[] originalSetterInstructions)
        {
            try
            {
                var propTypeRef = propDef.PropertyType;
                var loaderTypeRef = MakeGenericType(moduleDef.ImportReference(typeof(IEnumerable<>)), propTypeRef);
                var loaderName = $"__load_{propDef.Name}";
                var loaderFieldDef = new FieldDefinition(loaderName, Mono.Cecil.FieldAttributes.Private, loaderTypeRef);
                if (typeDef.Fields.Any(x => x.Name == loaderName))
                {
                    return;
                }

                typeDef.Fields.Add(loaderFieldDef);

                var enumerableTypeDef = moduleDef.ImportReference(typeof(Enumerable)).Resolve();
                var singleOrDefaultMethodRef = moduleDef.ImportReference(enumerableTypeDef.Methods.Single(x => x.Name == "SingleOrDefault" && x.Parameters.Count == 1));
                // var singleOrDefault = loader.FieldType.Resolve().Methods.Single(x => x.Name == "SingleOrDefault");
                singleOrDefaultMethodRef = MakeGenericMethodReference(singleOrDefaultMethodRef, propDef.PropertyType);

                var getterMethodDefinition = propDef.GetMethod;
                var ilp = getterMethodDefinition.Body.GetILProcessor();
                var first = getterMethodDefinition.Body.Instructions[0];
                var backingField = (FieldReference)getterMethodDefinition.Body.Instructions.First(x => x.OpCode == OpCodes.Ldfld && ((FieldReference)x.Operand).FieldType == propDef.PropertyType).Operand;
                var ret = getterMethodDefinition.Body.Instructions.Last();

                // var setter = pd.SetMethod;
                // var setterInstructions = setter.Body.Instructions;

                getterMethodDefinition.Body.Variables.Add(new VariableDefinition(propTypeRef));
                getterMethodDefinition.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(bool))));
                getterMethodDefinition.Body.Variables.Add(new VariableDefinition(propTypeRef));
                getterMethodDefinition.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(bool))));

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
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, loaderFieldDef));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldnull));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Cgt_Un));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_3));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_3));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Brfalse_S, il_0038));

                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, loaderFieldDef));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, singleOrDefaultMethodRef));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stloc_0));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
                ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_0));
                // ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, pr.SetMethod));
                if (originalSetterInstructions != null)
                {
                    foreach (var si in originalSetterInstructions.Skip(2))
                    {
                        if (si.OpCode == OpCodes.Ret) { break; }

                        ilp.InsertBefore(ret, si);
                    }
                }
                else
                {
                    ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stfld, backingField));
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

        private (TypeDefinition, FieldDefinition) MakeMetadataType(ModuleDefinition moduleDef, TypeDefinition typeDef)
        {
            var fieldTypeDef = new TypeDefinition(typeDef.Namespace, $"__Metadata__", Mono.Cecil.TypeAttributes.NestedPrivate | Mono.Cecil.TypeAttributes.AutoLayout | Mono.Cecil.TypeAttributes.AnsiClass | Mono.Cecil.TypeAttributes.BeforeFieldInit, moduleDef.ImportReference(moduleDef.TypeSystem.Object));
            fieldTypeDef.Interfaces.Add(new InterfaceImplementation(moduleDef.ImportReference(typeof(IPersistableMetadata))));

            var isNewFieldDef = new FieldDefinition("_isNew", Mono.Cecil.FieldAttributes.Private, moduleDef.ImportReference(moduleDef.TypeSystem.Boolean));
            fieldTypeDef.Fields.Add(isNewFieldDef);

            return (fieldTypeDef, isNewFieldDef);
        }

        /// <summary>
        ///   プロパティの変更時にdirtyフラグをONにする処理を差し込む。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="propDef">対象となるプロパティ定義情報。</param>
        /// <param name="dirtyFieldDef">dirtyフラグの定義情報。</param>
        /// <param name="originalGetterInstructions">当該プロパティにおける変更前のgetterメソッドIL。</param>
        private void AddPropertyDirtySetter(ModuleDefinition moduleDef, TypeReference typeRef, PropertyDefinition propDef, FieldDefinition metadataFieldDef, FieldDefinition dirtyFieldDef, Instruction[] originalGetterInstructions)
        {
            var dictionaryTypeDef = moduleDef.ImportReference(typeof(IDictionary<,>)).Resolve();
            var setItemMethodRef = moduleDef.ImportReference(MakeHostInstanceGenericMethodReference(dictionaryTypeDef.Methods.Single(x => x.Name == "set_Item" && x.Parameters.Count == 2), moduleDef.ImportReference(typeof(string)), moduleDef.ImportReference(typeof(bool))));
            
            var monitorTypeDef = moduleDef.ImportReference(typeof(Monitor)).Resolve();
            var enterMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Enter" && x.Parameters.Count == 2));
            var exitMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Exit" && x.Parameters.Count == 1));

            // var opInequalityMethodRef = moduleDef.ImportReference(typeRef.Resolve().Methods.Single(x => x.Name == "op_Inequality"));

            var setterMethodDef = propDef.SetMethod;
            var ilp = setterMethodDef.Body.GetILProcessor();
            var first = setterMethodDef.Body.Instructions[0];
            var ret = setterMethodDef.Body.Instructions.Last();

            // var getterMethodDef = pd.GetMethod;
            // var getterInstructions = getterMethodDef.Body.Instructions;

            setterMethodDef.Body.Variables.Add(new VariableDefinition(typeRef));
            var v_1 = new VariableDefinition(moduleDef.ImportReference(typeof(bool)));
            setterMethodDef.Body.Variables.Add(v_1);
            setterMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(bool))));

            var il_002c = ret;

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldarg_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldfld, metadataFieldDef));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stloc_0));

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldc_I4_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stloc_1));

            // .try {
            var il_0005 = Instruction.Create(OpCodes.Ldloc_0);
            ilp.InsertBefore(first, il_0005);
            ilp.InsertBefore(first, ilp.Create(OpCodes.Ldloca_S, v_1));

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Call, enterMethodRef));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));

            // if (_field != value) {
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
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stloc_2));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldloc_2));
            var il_002f = Instruction.Create(OpCodes.Nop);
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Brfalse_S, il_002f));

            // _name = value;
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Nop));

            // original code here

            // (set dirty)
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldarg_0));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, metadataFieldDef));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldc_I4_1));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Stfld, dirtyFieldDef));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));

            // }
            ilp.InsertBefore(ret, il_002f);
            var il_003d = ret;
            var il_0030 = Instruction.Create(OpCodes.Leave_S, il_003d);
            ilp.InsertBefore(ret, il_0030);
            // end .try

            //
            var il_0032 = Instruction.Create(OpCodes.Ldloc_1);
            ilp.InsertBefore(ret, il_0032);
            var il_003c = Instruction.Create(OpCodes.Endfinally);
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Brfalse_S, il_003c));

            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldloc_0));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Call, exitMethodRef));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));

            var handler = new ExceptionHandler(ExceptionHandlerType.Finally)
            {
                TryStart = il_0005,
                TryEnd = il_0032,
                HandlerStart = il_0032,
                HandlerEnd = ret,
            };

            ilp.InsertBefore(ret, il_003c);

            setterMethodDef.Body.ExceptionHandlers.Add(handler);
            // end handler
        }

        /// <summary>
        ///   対象のプロパティにBackingFieldAttributeを付与する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="propDef">対象となるプロパティ定義情報。</param>
        private void AddPropertyBackingFieldAttribute(ModuleDefinition moduleDef, PropertyDefinition propDef)
        {
            var getterMethodDef = propDef.GetMethod;
            var backingFieldRef = (FieldReference)getterMethodDef.Body.Instructions.First(x => x.OpCode == OpCodes.Ldfld && ((FieldReference)x.Operand).FieldType.FullName == propDef.PropertyType.FullName).Operand;

            var backingFieldAttributeTypeDef = moduleDef.ImportReference(typeof(BackingFieldAttribute)).Resolve();
            var backingFieldAttributeCtor = backingFieldAttributeTypeDef.Methods.First(m => m.IsConstructor && m.Parameters.Count == 1);

            var cattr = new CustomAttribute(moduleDef.ImportReference(backingFieldAttributeCtor));
            cattr.ConstructorArguments.Add(new CustomAttributeArgument(moduleDef.ImportReference(typeof(string)).Resolve(), backingFieldRef.Name));

            propDef.CustomAttributes.Add(cattr);
        }

        /// <summary>
        ///   プロパティの変更フラグを持つdirtyフィールドを作成する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <returns>dirtyフィールドを示す<see cref="FieldDefinition"/>。もしすでに作成済みの型であればnullを返す。</returns>
        private FieldDefinition MakeDirtyField(ModuleDefinition moduleDef, TypeDefinition typeDef, string name)
        {
            var dirtyFieldName = $"<{name}>k__Dirty";

            if (typeDef.Fields.Any(x => x.Name == dirtyFieldName))
            {
                return null;
            }

            var dirtyFieldDef = new FieldDefinition(dirtyFieldName, Mono.Cecil.FieldAttributes.Public, moduleDef.ImportReference(typeof(bool)));
            // typeDef.Fields.Add(dirtyFieldDef);

            return dirtyFieldDef;
        }

        /// <summary>
        ///   <see cref="IPersistable.GetDirtyState"/>メソッドを対象の型に追加する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="dirtyFieldDefsAndNames">メソッド内で参照する<c>__dirty</c>フィールド。</param>
        private void BuildMetadataProperty(ModuleDefinition moduleDef, TypeDefinition typeDef, TypeDefinition fieldTypeDef, FieldDefinition metadataFieldDef, IEnumerable<FieldDefinition> dirtyFieldDefs, FieldDefinition isNewFieldDef)
        {
            var monitorTypeDef = moduleDef.ImportReference(typeof(Monitor)).Resolve();
            var enterMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Enter" && x.Parameters.Count == 2));
            var exitMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Exit" && x.Parameters.Count == 1));

            foreach (var dirtyFieldDef in dirtyFieldDefs)
            {
                // var dirtyFieldDef = new FieldDefinition($"<{fdef.Name}>k__Dirty", Mono.Cecil.FieldAttributes.Private, moduleDef.TypeSystem.Boolean);
                fieldTypeDef.Fields.Add(dirtyFieldDef);
            }

            var metadataPropertyDef = new PropertyDefinition("__Metadata", Mono.Cecil.PropertyAttributes.None, moduleDef.ImportReference(typeof(IPersistableMetadata)));
            var getterMethodDef = new MethodDefinition("get___Metadata", Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Final | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName | Mono.Cecil.MethodAttributes.NewSlot | Mono.Cecil.MethodAttributes.Virtual, moduleDef.ImportReference(typeof(IPersistableMetadata)));
            metadataPropertyDef.GetMethod = getterMethodDef;

            var ilp = getterMethodDef.Body.GetILProcessor();

            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Ldfld, metadataFieldDef));
            ilp.Append(Instruction.Create(OpCodes.Ret));

            var typeCtorRef = typeDef.Methods.Single(x => x.Name == ".ctor");
            var fieldTypeCtorRef = new MethodDefinition(".ctor", Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName | Mono.Cecil.MethodAttributes.RTSpecialName, moduleDef.TypeSystem.Void);
            fieldTypeDef.Methods.Add(fieldTypeCtorRef);

            var ilpCtor = typeCtorRef.Body.GetILProcessor();
            var first = typeCtorRef.Body.Instructions.First();

            ilpCtor.InsertBefore(first, Instruction.Create(OpCodes.Ldarg_0));
            ilpCtor.InsertBefore(first, Instruction.Create(OpCodes.Newobj, fieldTypeCtorRef));
            ilpCtor.InsertBefore(first, Instruction.Create(OpCodes.Stfld, metadataFieldDef));

            //var ilpFieldCtor = fieldTypeCtorRef.Body.GetILProcessor();
            //var ctorFirst = fieldTypeCtorRef.Body.Instructions.First();

            var ilpMetadataCtor = fieldTypeCtorRef.Body.GetILProcessor();

            foreach (var dirtyFieldDef in dirtyFieldDefs)
            {
                ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ldarg_0));
                ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ldc_I4_0));
                ilpMetadataCtor.Append(Instruction.Create(OpCodes.Stfld, dirtyFieldDef));
            }
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ldc_I4_1));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Stfld, isNewFieldDef));

            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Call, moduleDef.ImportReference(fieldTypeDef.BaseType.Resolve().Methods.Single(x => x.Name == ".ctor"))));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Nop));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Nop));
            ilpMetadataCtor.Append(Instruction.Create(OpCodes.Ret));

            typeDef.Methods.Add(getterMethodDef);
            typeDef.Properties.Add(metadataPropertyDef);
            typeDef.Fields.Add(metadataFieldDef);

            typeDef.NestedTypes.Add(fieldTypeDef);
        }

        /// <summary>
        ///   <see cref="IPersistable.GetDirtyState"/>メソッドを対象の型に追加する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="dirtyFieldDefsAndNames">メソッド内で参照する<c>__dirty</c>フィールド。</param>
        private void AddGetDirtyState(ModuleDefinition moduleDef, TypeDefinition typeDef, TypeDefinition metadataTypeDef, FieldDefinition metadataFieldDef, IEnumerable<(FieldDefinition, string)> dirtyFieldDefsAndNames)
        {
            var monitorTypeDef = moduleDef.ImportReference(typeof(Monitor)).Resolve();
            var enterMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Enter" && x.Parameters.Count == 2));
            var exitMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Exit" && x.Parameters.Count == 1));

            var getDirtyStateMethodDef = new MethodDefinition(nameof(IPersistableMetadata.GetDirtyState), Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Virtual | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName, moduleDef.ImportReference(typeof(PersistableState)));
            var ilp = getDirtyStateMethodDef.Body.GetILProcessor();

            getDirtyStateMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(PersistableState))));
            getDirtyStateMethodDef.Body.Variables.Add(new VariableDefinition(typeDef));
            var v_2 = new VariableDefinition(moduleDef.ImportReference(typeof(bool)));
            getDirtyStateMethodDef.Body.Variables.Add(v_2);
            getDirtyStateMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(PersistableState))));

            var persistableStateCtorRef = moduleDef.ImportReference(moduleDef.ImportReference(typeof(PersistableStateImpl)).Resolve().Methods.Single(x => x.Name == ".ctor"));

            ilp.Append(Instruction.Create(OpCodes.Nop));
            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Stloc_1));

            // {
            ilp.Append(Instruction.Create(OpCodes.Ldc_I4_0));
            ilp.Append(Instruction.Create(OpCodes.Stloc_2));
            // .try
            var il_0005 = Instruction.Create(OpCodes.Ldloc_1);
            ilp.Append(il_0005);
            ilp.Append(Instruction.Create(OpCodes.Ldloca_S, v_2));

            ilp.Append(Instruction.Create(OpCodes.Call, enterMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Nop));

            // if (_field != value) {
            ilp.Append(Instruction.Create(OpCodes.Nop));

            ilp.Append(CreateInstructionIdcI4(dirtyFieldDefsAndNames.Count()));
            ilp.Append(Instruction.Create(OpCodes.Newarr, moduleDef.ImportReference(typeof(string))));

            int i = 0;
            foreach (var (fieldDef, fieldName) in dirtyFieldDefsAndNames)
            {
                ilp.Append(Instruction.Create(OpCodes.Dup));
                ilp.Append(CreateInstructionIdcI4(i++));
                ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
                ilp.Append(Instruction.Create(OpCodes.Ldfld, fieldDef));
                var il_0022 = Instruction.Create(OpCodes.Ldstr, fieldName);
                ilp.Append(Instruction.Create(OpCodes.Brtrue_S, il_0022));
                ilp.Append(Instruction.Create(OpCodes.Ldnull));
                var il_0027 = Instruction.Create(OpCodes.Stelem_Ref);
                ilp.Append(Instruction.Create(OpCodes.Br_S, il_0027));
                ilp.Append(il_0022);
                ilp.Append(il_0027);
            }

            ilp.Append(Instruction.Create(OpCodes.Newobj, persistableStateCtorRef));
            ilp.Append(Instruction.Create(OpCodes.Stloc_0));
            //

            ilp.Append(Instruction.Create(OpCodes.Nop));
            var il_005d = Instruction.Create(OpCodes.Ldloc_0);
            ilp.Append(Instruction.Create(OpCodes.Leave_S, il_005d));
            // end .try

            var il_0052 = Instruction.Create(OpCodes.Ldloc_2);
            ilp.Append(il_0052);
            var il_005c = Instruction.Create(OpCodes.Endfinally);
            ilp.Append(Instruction.Create(OpCodes.Brfalse_S, il_005c));

            ilp.Append(Instruction.Create(OpCodes.Ldloc_1));
            ilp.Append(Instruction.Create(OpCodes.Call, exitMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Nop));

            ilp.Append(il_005c);
            // end handler

            ilp.Append(il_005d);
            ilp.Append(Instruction.Create(OpCodes.Stloc_3));
            var il_0061 = Instruction.Create(OpCodes.Ldloc_3);
            ilp.Append(Instruction.Create(OpCodes.Br_S, il_0061));

            ilp.Append(il_0061);
            var ret = Instruction.Create(OpCodes.Ret);
            ilp.Append(ret);

            var handler = new ExceptionHandler(ExceptionHandlerType.Finally)
            {
                TryStart = il_0005,
                TryEnd = il_0052,
                HandlerStart = il_0052,
                HandlerEnd = il_005d,
            };

            getDirtyStateMethodDef.Body.ExceptionHandlers.Add(handler);
            metadataTypeDef.Methods.Add(getDirtyStateMethodDef);
        }

        /// <summary>
        ///   <see cref="IPersistable.GetDirtyState"/>メソッドを対象の型に追加する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="dirtyFieldDefsAndNames">メソッド内で参照する<c>__dirty</c>フィールド。</param>
        private void AddResetDirtyState(ModuleDefinition moduleDef, TypeDefinition typeDef, TypeDefinition metadataTypeDef, FieldDefinition metadataFieldDef, IEnumerable<(FieldDefinition, string)> dirtyFieldDefsAndNames, FieldDefinition isNewFieldDef)
        {
            var monitorTypeDef = moduleDef.ImportReference(typeof(Monitor)).Resolve();
            var enterMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Enter" && x.Parameters.Count == 2));
            var exitMethodRef = moduleDef.ImportReference(monitorTypeDef.Methods.Single(x => x.Name == "Exit" && x.Parameters.Count == 1));

            var getDirtyStateMethodDef = new MethodDefinition(nameof(IPersistableMetadata.ResetDirtyState), Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Virtual | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName, moduleDef.ImportReference(typeof(void)));
            var ilp = getDirtyStateMethodDef.Body.GetILProcessor();

            getDirtyStateMethodDef.Body.Variables.Add(new VariableDefinition(typeDef));
            var v_1 = new VariableDefinition(moduleDef.ImportReference(typeof(bool)));
            getDirtyStateMethodDef.Body.Variables.Add(v_1);

            ilp.Append(Instruction.Create(OpCodes.Nop));
            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Stloc_0));

            // {
            ilp.Append(Instruction.Create(OpCodes.Ldc_I4_0));
            ilp.Append(Instruction.Create(OpCodes.Stloc_1));
            // .try
            var il_0005 = Instruction.Create(OpCodes.Ldloc_0);
            ilp.Append(il_0005);
            ilp.Append(Instruction.Create(OpCodes.Ldloca_S, v_1));

            ilp.Append(Instruction.Create(OpCodes.Call, enterMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Nop));

            ilp.Append(Instruction.Create(OpCodes.Nop));

            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Ldc_I4_0));
            ilp.Append(Instruction.Create(OpCodes.Stfld, isNewFieldDef));

            //
            foreach (var (fieldDef, fieldName) in dirtyFieldDefsAndNames)
            {
                ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
                ilp.Append(Instruction.Create(OpCodes.Ldc_I4_0));
                ilp.Append(Instruction.Create(OpCodes.Stfld, fieldDef));
            }

            ilp.Append(Instruction.Create(OpCodes.Nop));
            var il_002b = Instruction.Create(OpCodes.Ret);
            ilp.Append(Instruction.Create(OpCodes.Leave_S, il_002b));
            // end .try

            var il_0052 = Instruction.Create(OpCodes.Ldloc_1);
            ilp.Append(il_0052);
            var il_005c = Instruction.Create(OpCodes.Endfinally);
            ilp.Append(Instruction.Create(OpCodes.Brfalse_S, il_005c));

            ilp.Append(Instruction.Create(OpCodes.Ldloc_0));
            ilp.Append(Instruction.Create(OpCodes.Call, exitMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Nop));

            ilp.Append(il_005c);
            // end handler

            ilp.Append(il_002b);

            var handler = new ExceptionHandler(ExceptionHandlerType.Finally)
            {
                TryStart = il_0005,
                TryEnd = il_0052,
                HandlerStart = il_0052,
                HandlerEnd = il_002b,
            };

            getDirtyStateMethodDef.Body.ExceptionHandlers.Add(handler);
            metadataTypeDef.Methods.Add(getDirtyStateMethodDef);
        }

        /// <summary>
        ///   <see cref="IPersistable.GetDirtyState"/>メソッドを対象の型に追加する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="dirtyFieldDefsAndNames">メソッド内で参照する<c>__dirty</c>フィールド。</param>
        private void AddPropertyIsNew(ModuleDefinition moduleDef, TypeDefinition metadataTypeDef, FieldDefinition isNewFieldDef)
        {
            var isNewMethodDef = new MethodDefinition("get_IsNew", Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Final | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName | Mono.Cecil.MethodAttributes.NewSlot | Mono.Cecil.MethodAttributes.Virtual, moduleDef.ImportReference(moduleDef.TypeSystem.Boolean));
            var ilp = isNewMethodDef.Body.GetILProcessor();

            var isNewPropertyDef = new PropertyDefinition("IsNew", Mono.Cecil.PropertyAttributes.None, moduleDef.ImportReference(moduleDef.TypeSystem.Boolean));
            isNewPropertyDef.GetMethod = isNewMethodDef;

            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Ldfld, isNewFieldDef));
            ilp.Append(Instruction.Create(OpCodes.Ret));

            metadataTypeDef.Methods.Add(isNewMethodDef);
            metadataTypeDef.Properties.Add(isNewPropertyDef);
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

        /// <summary>
        ///   ジェネリック型の型情報をもとにインスタンス化された型情報を生成する。
        /// </summary>
        /// <param name="self">対象のジェネリック型。</param>
        /// <param name="arguments">型引数。</param>
        /// <returns>インスタンス化された型参照。</returns>
        /// <exception cref="ArgumentException">要求された型引数と<paramref name="arguments"/>の個数が一致しない場合。</exception>
        private static MethodReference MakeGenericMethodReference(MethodReference self, params TypeReference[] arguments)
        {
            if (self.GenericParameters.Count != arguments.Length)
            {
                throw new ArgumentException();
            }

            var instance = new GenericInstanceMethod(self);
            foreach (var argument in arguments)
            {
                instance.GenericArguments.Add(argument);
            }

            return instance;
        }

        /// <summary>
        ///   ジェネリックメソッドの参照情報をもとにインスタンス化されたメソッド参照情報を生成する。
        /// </summary>
        /// <param name="self">対象のジェネリックメソッド参照情報。</param>
        /// <param name="genericArguments">型引数。</param>
        /// <returns>インスタンス化されたメソッド参照。</returns>
        private static MethodReference MakeHostInstanceGenericMethodReference(MethodReference self, params TypeReference[] genericArguments)
        {
            var genericDeclaringType = new GenericInstanceType(self.DeclaringType);
            foreach (var genericArgument in genericArguments)
            {
                genericDeclaringType.GenericArguments.Add(genericArgument);
            }

            var methodRef = new MethodReference(self.Name, self.ReturnType, genericDeclaringType)
            {
                HasThis = self.HasThis,
                ExplicitThis = self.ExplicitThis,
                CallingConvention = self.CallingConvention
            };

            foreach (var parameter in self.Parameters)
            {
                methodRef.Parameters.Add(new ParameterDefinition(parameter.ParameterType));
            }

            foreach (var genericParam in self.GenericParameters)
            {
                methodRef.GenericParameters.Add(new GenericParameter(genericParam.Name, methodRef));
            }

            return methodRef;
        }

        private Instruction CreateInstructionIdcI4(int n)
        {
            switch (n)
            {
                case 0: return Instruction.Create(OpCodes.Ldc_I4_0);
                case 1: return Instruction.Create(OpCodes.Ldc_I4_1);
                case 2: return Instruction.Create(OpCodes.Ldc_I4_2);
                case 3: return Instruction.Create(OpCodes.Ldc_I4_3);
                case 4: return Instruction.Create(OpCodes.Ldc_I4_4);
                case 5: return Instruction.Create(OpCodes.Ldc_I4_5);
                case 6: return Instruction.Create(OpCodes.Ldc_I4_6);
                case 7: return Instruction.Create(OpCodes.Ldc_I4_7);
                case 8: return Instruction.Create(OpCodes.Ldc_I4_8);
                default: return Instruction.Create(OpCodes.Ldc_I4, n);
            }
        }
    }

    public class PersistableStateImpl : PersistableState
    {
        public PersistableStateImpl(string[] dirtyKeys) : base(dirtyKeys.Where(x => !string.IsNullOrEmpty(x)).ToArray())
        {
        }
    }
}
