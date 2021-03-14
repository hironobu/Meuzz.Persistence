using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Meuzz.Persistence
{
    public class ReflectionEmit
    {
        private TypeBuilder _typeBuilder;
        private Type _objectType;
    
        private List<PropertyInfo> _props = new List<PropertyInfo>();
        private IDictionary<string, Delegate> _propLoaders = new Dictionary<string, Delegate>();


        /*public Type CreateTypeOverride(Type originalType, PropertyInfo originalProp, Delegate closureInfo)
        {
            var aName = Assembly.GetExecutingAssembly().GetName();

            GetDynamicObject(aName, originalType, originalProp, closureInfo);
        }*/

        public void BuildStart(AssemblyName assembly, Type objectType)
        {
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assembly, AssemblyBuilderAccess.Run);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assembly.Name);

            //create the class
            _objectType = objectType;
            _typeBuilder = moduleBuilder.DefineType(objectType.Name, TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                                                                TypeAttributes.BeforeFieldInit, objectType);

        }

        public void BuildProperty(PropertyInfo prop, Delegate propLoader)
        {
            var loaderName = "__" + prop.Name + "Loader";
            FieldBuilder fieldBuilder = _typeBuilder.DefineField(loaderName, typeof(Func<,>).MakeGenericType(_objectType, prop.PropertyType), FieldAttributes.Public | FieldAttributes.Static);

            MethodBuilder pGet = _typeBuilder.DefineMethod("get_" + prop.Name, MethodAttributes.NewSlot | MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, prop.PropertyType, Type.EmptyTypes);
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

            var label_IL_0015 = pILGet.DefineLabel();
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
            pILGet.Emit(OpCodes.Ldsfld, fieldBuilder);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Callvirt, typeof(Func<,>).MakeGenericType(_objectType, prop.PropertyType).GetMethod("Invoke"));
            pILGet.Emit(OpCodes.Stloc, s0);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Ldloc, s0);
            pILGet.EmitCall(OpCodes.Call, prop.GetSetMethod(), null);
            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldloc, s0);
            pILGet.Emit(OpCodes.Stloc, s2);
            pILGet.Emit(OpCodes.Br_S, label_IL_002d);
            pILGet.MarkLabel(label_IL_002d);
            pILGet.Emit(OpCodes.Ldloc, s2);
            pILGet.Emit(OpCodes.Ret);
#endif

            PropertyBuilder newProp = _typeBuilder.DefineProperty(prop.Name, PropertyAttributes.None, prop.PropertyType, Type.EmptyTypes);
            newProp.SetGetMethod(pGet);

            _propLoaders.Add(loaderName, propLoader);
        }


        public Type BuildFinish()
        {
            var returnType = _typeBuilder.CreateType();

            foreach (var (name, loader) in _propLoaders)
            {
                var f = returnType.GetField(name);
                // f.SetValue(null, loader);
            }

            return returnType;
        }

    }
}