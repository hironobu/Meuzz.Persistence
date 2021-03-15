using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Meuzz.Persistence
{
    public class ProxyTypeBuilder
    {
        private TypeBuilder _typeBuilder;
        private Type _objectType;

        public void BuildStart(AssemblyName assembly, Type objectType)
        {
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assembly, AssemblyBuilderAccess.Run);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assembly.Name);

            //create the class
            _objectType = objectType;
            _typeBuilder = moduleBuilder.DefineType(objectType.Name, TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                                                                TypeAttributes.BeforeFieldInit, objectType);

        }

        public void BuildOverrideProperty(PropertyInfo prop)
        {
            // var loaderName = "__" + prop.Name + "Loader";
            FieldBuilder fieldBuilder = _typeBuilder.DefineField(loaderName, typeof(Func<,>).MakeGenericType(_objectType, prop.PropertyType), FieldAttributes.Public);

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
            pILGet.Emit(OpCodes.Ldfld, fieldBuilder);
            pILGet.Emit(OpCodes.Ldnull);
            pILGet.Emit(OpCodes.Cgt_Un);
            pILGet.Emit(OpCodes.Stloc, s3);
            pILGet.Emit(OpCodes.Ldloc, s3);
            pILGet.Emit(OpCodes.Brfalse_S, label_IL_0039);
            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Ldfld, fieldBuilder);
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


        public Type BuildFinish()
        {
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