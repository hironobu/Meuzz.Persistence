using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Meuzz.Persistence.Reflections
{
    [Obsolete]
    public class PersistentTypeBuilder
    {
        private TypeBuilder _typeBuilder;
        private TypeBuilder _loaderTypeBuilder;
        private FieldBuilder _loaderField;
        private Type _objectType;
        private IDictionary<PropertyInfo, FieldBuilder> _propertyLoaders = new Dictionary<PropertyInfo, FieldBuilder>();

        public void BuildStart(Type objectType)
        {
            AssemblyName assembly = Assembly.GetExecutingAssembly().GetName();
            BuildStart(assembly, objectType);
        }

        public void BuildStart(AssemblyName assembly, Type objectType)
        {
            // AssemblyName assembly = Assembly.GetExecutingAssembly().GetName();
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assembly, AssemblyBuilderAccess.Run);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assembly.Name);

            //create the class
            _objectType = objectType;
            _typeBuilder = moduleBuilder.DefineType(objectType.Name, TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                                                                TypeAttributes.BeforeFieldInit, objectType);
            foreach (var f in objectType.GetFields())
            {
                _typeBuilder.DefineField(f.Name, f.FieldType, FieldAttributes.Private);
            }

            _loaderTypeBuilder = moduleBuilder.DefineType($"{objectType.Name}Loader", TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                TypeAttributes.BeforeFieldInit);
        }

        public void BuildOverrideProperty(PropertyInfo prop)
        {
            FieldBuilder propLoaderField = _loaderTypeBuilder.DefineField(prop.Name, typeof(Func<,>).MakeGenericType(_objectType, prop.PropertyType), FieldAttributes.Public);

            _propertyLoaders.Add(prop, propLoaderField);
        }

        public Type BuildFinish()
        {
            ConstructorBuilder loaderCtorBuilder = _loaderTypeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, new Type[] { });
            ILGenerator pilLoader = loaderCtorBuilder.GetILGenerator();

            foreach (var (prop, loader) in _propertyLoaders)
            {
                pilLoader.Emit(OpCodes.Ldarg_0);
                pilLoader.Emit(OpCodes.Ldnull);
                pilLoader.Emit(OpCodes.Stfld, loader);
            }
            pilLoader.Emit(OpCodes.Ldarg_0);
            pilLoader.Emit(OpCodes.Call, typeof(Object).GetConstructor(new Type[] { }));
            pilLoader.Emit(OpCodes.Nop);
            pilLoader.Emit(OpCodes.Ret);

            _loaderField = _typeBuilder.DefineField("__Loader__", _loaderTypeBuilder.CreateType(), FieldAttributes.Public);

            foreach (var (prop, loader) in _propertyLoaders)
            {
                MethodBuilder pGet = _typeBuilder.DefineMethod("get_" + prop.Name, MethodAttributes.Virtual | MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, prop.PropertyType, Type.EmptyTypes);
                ILGenerator pILGet = pGet.GetILGenerator();

#if false
                pILGet.Emit(OpCodes.Nop);
                var ctor = typeof(System.NotImplementedException).GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new Type[] { }, null);
                pILGet.Emit(OpCodes.Newobj, ctor);
                pILGet.Emit(OpCodes.Throw);
#else
                var s0 = pILGet.DeclareLocal(prop.PropertyType);
                var s1 = pILGet.DeclareLocal(typeof(Int32));
                var s2 = pILGet.DeclareLocal(prop.PropertyType);
                var s3 = pILGet.DeclareLocal(typeof(bool));

                var label_IL_0015 = pILGet.DefineLabel();
                var label_IL_0039 = pILGet.DefineLabel();
                var label_IL_002d = pILGet.DefineLabel();

                pILGet.Emit(OpCodes.Nop);
                pILGet.Emit(OpCodes.Ldarg_0);
                pILGet.EmitCall(OpCodes.Call, prop.GetGetMethod(), null);
                pILGet.Emit(OpCodes.Stloc, s0);
                pILGet.Emit(OpCodes.Ldloc, s0);
                pILGet.Emit(OpCodes.Ldnull);
                pILGet.Emit(OpCodes.Cgt_Un);
                pILGet.Emit(OpCodes.Stloc, s1);
                pILGet.Emit(OpCodes.Ldloc, s1);
                pILGet.Emit(OpCodes.Brfalse_S, label_IL_0015);
                pILGet.Emit(OpCodes.Nop);
                pILGet.Emit(OpCodes.Ldloc, s0);
                pILGet.Emit(OpCodes.Stloc, s2);
                pILGet.Emit(OpCodes.Br_S, label_IL_002d);
                pILGet.MarkLabel(label_IL_0015);
                pILGet.Emit(OpCodes.Ldarg_0);
                pILGet.Emit(OpCodes.Ldfld, _loaderField);
                pILGet.Emit(OpCodes.Ldfld, loader);
                pILGet.Emit(OpCodes.Ldnull);
                pILGet.Emit(OpCodes.Cgt_Un);
                pILGet.Emit(OpCodes.Stloc, s3);
                pILGet.Emit(OpCodes.Ldloc, s3);
                pILGet.Emit(OpCodes.Brfalse_S, label_IL_0039);
                pILGet.Emit(OpCodes.Nop);
                pILGet.Emit(OpCodes.Ldarg_0);
                pILGet.Emit(OpCodes.Ldfld, _loaderField);
                pILGet.Emit(OpCodes.Ldfld, loader);
                pILGet.Emit(OpCodes.Ldarg_0);
                pILGet.Emit(OpCodes.Callvirt, typeof(Func<,>).MakeGenericType(_objectType, prop.PropertyType).GetMethod("Invoke"));
                pILGet.Emit(OpCodes.Stloc, s0);
                pILGet.Emit(OpCodes.Ldarg_0);
                pILGet.Emit(OpCodes.Ldloc, s0);
                pILGet.EmitCall(OpCodes.Call, prop.GetSetMethod(), null);
                pILGet.Emit(OpCodes.Nop);
                pILGet.Emit(OpCodes.Nop);
                pILGet.MarkLabel(label_IL_0039);
                pILGet.Emit(OpCodes.Ldloc, s0);
                pILGet.Emit(OpCodes.Stloc, s2);
                pILGet.Emit(OpCodes.Br_S, label_IL_002d);
                pILGet.MarkLabel(label_IL_002d);
                pILGet.Emit(OpCodes.Ldloc, s2);
                pILGet.Emit(OpCodes.Ret);
#endif

                PropertyBuilder newProp = _typeBuilder.DefineProperty(prop.Name, PropertyAttributes.None, prop.PropertyType, Type.EmptyTypes);
                newProp.SetGetMethod(pGet);
            }

            ConstructorBuilder ctorBuilder = _typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, new Type[] { });
            ILGenerator pil = ctorBuilder.GetILGenerator();

            pil.Emit(OpCodes.Ldarg_0);
            pil.Emit(OpCodes.Newobj, loaderCtorBuilder);
            pil.Emit(OpCodes.Stfld, _loaderField);
            pil.Emit(OpCodes.Ldarg_0);
            pil.Emit(OpCodes.Call, _objectType.GetConstructor(new Type[] { }));
            pil.Emit(OpCodes.Nop);
            pil.Emit(OpCodes.Ret);

            return _typeBuilder.CreateType();
        }
    }

    public class PersistentTypeManager
    {
        private IDictionary<Type, Type> _typeDict = new ConcurrentDictionary<Type, Type>();

        public Type GetPersistentType(Type t, Type persistentType = null)
        {
            if (_typeDict.ContainsKey(t))
            {
                return _typeDict[t];
            }
            if (_typeDict.Values.Contains(t))
            {
                return t;
            }

            if (persistentType == null)
            {
                throw new NotImplementedException();
            }

            _typeDict.Add(t, persistentType);
            return persistentType;
        }

        private static PersistentTypeManager _instance = null;
        private static readonly object _instanceLock = new object();

        public static PersistentTypeManager Instance()
        {
            if (_instance == null)
            {
                lock (_instanceLock)
                {
                    if (_instance == null)
                    {
                        _instance = new PersistentTypeManager();
                    }
                }
            }

            return _instance;
        }
    }


    public static class PersistentTypeExtensions
    {
        public static Type GetPersistentType(this Type t, Type persistentType = null)
        {
            return PersistentTypeManager.Instance().GetPersistentType(t, persistentType);
        }
    }
}