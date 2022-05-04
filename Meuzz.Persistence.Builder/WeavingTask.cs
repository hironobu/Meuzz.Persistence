using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
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

            // backup original assembly file
            var originalAssemblyFileName = $"{assemblyFileName}.orig";
            File.Delete(originalAssemblyFileName);
            File.Move(assemblyFileName, originalAssemblyFileName);

            var (mainModule, hasSymbols) = moduleManager.ReadModule(originalAssemblyFileName, false);

            var typeDefs = mainModule.GetTypes().Where(t => t.HasCustomAttributes && t.CustomAttributes.Any(ca => ca.AttributeType.FullName == typeof(PersistentClassAttribute).FullName));
            foreach (var typeDef in typeDefs)
            {
                WeaveTypeAsPersistent(mainModule, typeDef);
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

        /// <summary>
        ///   対象の型に永続化のための処理(weave)を行う。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        private void WeaveTypeAsPersistent(ModuleDefinition moduleDef, TypeDefinition typeDef)
        {
            var dirtyDictFieldDef = MakeDirtyField(moduleDef, typeDef);

            AddGeneratePersistentState(moduleDef, typeDef, dirtyDictFieldDef);

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
                    AddPropertyDirtySetter(moduleDef, pr, dirtyDictFieldDef, originalGetterInstructions);
                }
            }
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

        /// <summary>
        ///   プロパティの変更時にdirtyフラグをONにする処理を差し込む。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="propDef">対象となるプロパティ定義情報。</param>
        /// <param name="dirtyDictFieldDef">dirtyフラグの定義情報。</param>
        /// <param name="originalGetterInstructions">当該プロパティにおける変更前のgetterメソッドIL。</param>
        private void AddPropertyDirtySetter(ModuleDefinition moduleDef, PropertyDefinition propDef, FieldDefinition dirtyDictFieldDef, Instruction[] originalGetterInstructions)
        {
            var dictionaryTypeDef = moduleDef.ImportReference(typeof(IDictionary<,>)).Resolve();
            var setItemMethodRef = moduleDef.ImportReference(MakeHostInstanceGenericMethodReference(dictionaryTypeDef.Methods.Single(x => x.Name == "set_Item" && x.Parameters.Count == 2), moduleDef.ImportReference(typeof(string)), moduleDef.ImportReference(typeof(bool))));

            var setterMethodDef = propDef.SetMethod;
            var ilp = setterMethodDef.Body.GetILProcessor();
            var first = setterMethodDef.Body.Instructions[0];
            var ret = setterMethodDef.Body.Instructions.Last();

            // var getterMethodDef = pd.GetMethod;
            // var getterInstructions = getterMethodDef.Body.Instructions;

            setterMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(bool))));

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
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldfld, dirtyDictFieldDef));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldstr, propDef.Name));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Ldc_I4_1));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Callvirt, setItemMethodRef));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
            ilp.InsertBefore(ret, Instruction.Create(OpCodes.Nop));
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
        private FieldDefinition MakeDirtyField(ModuleDefinition moduleDef, TypeDefinition typeDef)
        {
            var dirtyDictName = "__dirty";

            if (typeDef.Fields.Any(x => x.Name == dirtyDictName))
            {
                return null;
            }

            var dirtyDictTypeRef = moduleDef.ImportReference(MakeGenericType(moduleDef.ImportReference(typeof(IDictionary<,>)), moduleDef.ImportReference(typeof(string)), moduleDef.ImportReference(typeof(bool))));
            var dirtyDictFieldDef = new FieldDefinition(dirtyDictName, Mono.Cecil.FieldAttributes.Private, dirtyDictTypeRef);
            typeDef.Fields.Add(dirtyDictFieldDef);

            var dirtyDictTypeDef = moduleDef.ImportReference(typeof(Dictionary<,>)).Resolve();
            var dirtyDictInstanceCtor = moduleDef.ImportReference(MakeHostInstanceGenericMethodReference(dirtyDictTypeDef.Methods.Single(x => x.Name == ".ctor" && x.Parameters.Count == 0), moduleDef.ImportReference(typeof(string)), moduleDef.ImportReference(typeof(bool))));
            var ctorMethodDef = typeDef.Methods.Single(x => x.Name == ".ctor");
            var ilp = ctorMethodDef.Body.GetILProcessor();
            var first = ctorMethodDef.Body.Instructions[0];

            ilp.InsertBefore(first, Instruction.Create(OpCodes.Ldarg_0));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Newobj, dirtyDictInstanceCtor));
            ilp.InsertBefore(first, Instruction.Create(OpCodes.Stfld, dirtyDictFieldDef));

            return dirtyDictFieldDef;
        }

        /// <summary>
        ///   <see cref="IPersistable.GeneratePersistableState"/>メソッドを対象の型に追加する。
        /// </summary>
        /// <param name="moduleDef">メインモジュール。</param>
        /// <param name="typeDef">対象となる型定義情報。</param>
        /// <param name="dirtyDictFieldDef">メソッド内で参照する<c>__dirty</c>フィールド。</param>
        private void AddGeneratePersistentState(ModuleDefinition moduleDef, TypeDefinition typeDef, FieldDefinition dirtyDictFieldDef)
        {
            var getKeysMethodRef = moduleDef.ImportReference(MakeHostInstanceGenericMethodReference(moduleDef.ImportReference(typeof(IDictionary<,>)).Resolve().Methods.Single(x => x.Name == "get_Keys"), moduleDef.ImportReference(typeof(string)), moduleDef.ImportReference(typeof(bool))));
            var toArrayMethodRef = moduleDef.ImportReference(MakeGenericMethodReference(moduleDef.ImportReference(typeof(Enumerable)).Resolve().Methods.Single(x => x.Name == nameof(Enumerable.ToArray)), moduleDef.ImportReference(typeof(string))));

            var generatePersistableStateMethodDef = new MethodDefinition(nameof(IPersistable.GeneratePersistableState), Mono.Cecil.MethodAttributes.Public | Mono.Cecil.MethodAttributes.Virtual | Mono.Cecil.MethodAttributes.HideBySig | Mono.Cecil.MethodAttributes.SpecialName, moduleDef.ImportReference(typeof(PersistableState)));
            var ilp = generatePersistableStateMethodDef.Body.GetILProcessor();

            generatePersistableStateMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(string[]))));
            generatePersistableStateMethodDef.Body.Variables.Add(new VariableDefinition(moduleDef.ImportReference(typeof(PersistableState))));

            var persistableStateCtorRef = moduleDef.ImportReference(moduleDef.ImportReference(typeof(PersistableState)).Resolve().Methods.Single(x => x.Name == ".ctor"));
            var clearMethodRef = moduleDef.ImportReference(moduleDef.ImportReference(typeof(IDictionary)).Resolve().Methods.Single(x => x.Name == nameof(IDictionary.Clear)));

            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Ldfld, dirtyDictFieldDef));
            ilp.Append(Instruction.Create(OpCodes.Callvirt, getKeysMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Call, toArrayMethodRef));
            ilp.Append(Instruction.Create(OpCodes.Stloc_0));

            ilp.Append(Instruction.Create(OpCodes.Ldarg_0));
            ilp.Append(Instruction.Create(OpCodes.Ldfld, dirtyDictFieldDef));
            ilp.Append(Instruction.Create(OpCodes.Callvirt, clearMethodRef));

            ilp.Append(Instruction.Create(OpCodes.Nop));
            ilp.Append(Instruction.Create(OpCodes.Ldloc_0));
            ilp.Append(Instruction.Create(OpCodes.Newobj, persistableStateCtorRef));
            ilp.Append(Instruction.Create(OpCodes.Stloc_1));

            var il_0027 = Instruction.Create(OpCodes.Ldloc_1);
            ilp.Append(Instruction.Create(OpCodes.Br_S, il_0027));

            ilp.Append(il_0027);
            ilp.Append(Instruction.Create(OpCodes.Ret));

            typeDef.Methods.Add(generatePersistableStateMethodDef);
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
    }
}
