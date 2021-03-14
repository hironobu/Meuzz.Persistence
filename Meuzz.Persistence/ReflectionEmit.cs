using System;
using System.Reflection;
using System.Reflection.Emit;

namespace Meuzz.Persistence
{
    public class ReflectionEmit
    {

        public Type CreateTypeOverride(Type originalType, PropertyInfo originalProp, Delegate closureInfo)
        {
            var aName = Assembly.GetExecutingAssembly().GetName();

            return GetDynamicObject(aName, originalType, originalProp, closureInfo);
        }

        public static Type GetDynamicObject(AssemblyName assembly, Type objectType, PropertyInfo prop, Delegate propLoader)
        {
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assembly, AssemblyBuilderAccess.Run);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assembly.Name);

            //create the class
            var typeBuilder = moduleBuilder.DefineType(objectType.Name, TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                                                                TypeAttributes.BeforeFieldInit, objectType);

            FieldBuilder fieldBuilder = typeBuilder.DefineField("__" + prop.Name + "Loader", typeof(Func<,>).MakeGenericType(objectType, prop.PropertyType), FieldAttributes.Public | FieldAttributes.Static);

            MethodBuilder pGet = typeBuilder.DefineMethod("get_" + prop.Name, MethodAttributes.NewSlot | MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, prop.PropertyType, Type.EmptyTypes);
            ILGenerator pILGet = pGet.GetILGenerator();

#if false
            pILGet.Emit(OpCodes.Nop);
            var ctor = typeof(System.NotImplementedException).GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new Type[] { }, null);
            pILGet.Emit(OpCodes.Newobj, ctor);
            pILGet.Emit(OpCodes.Throw);
#else
            // var ctor = propertyType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new Type[] { objectType }, null);

            var s0 = pILGet.DeclareLocal(prop.PropertyType);
            var s1 = pILGet.DeclareLocal(typeof(Int32));
            var s2 = pILGet.DeclareLocal(prop.PropertyType);

            var label_IL_0015 = pILGet.DefineLabel();
            var label_IL_001e = pILGet.DefineLabel();

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
            pILGet.Emit(OpCodes.Br_S, label_IL_001e);
            pILGet.MarkLabel(label_IL_0015);
            //pILGet.Emit(OpCodes.Newobj, ctor);
            //pILGet.EmitCall(OpCodes.Call, closureInfo, null);
            pILGet.Emit(OpCodes.Ldsfld, fieldBuilder);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Callvirt, typeof(Func<,>).MakeGenericType(objectType, prop.PropertyType).GetMethod("Invoke"));
            pILGet.Emit(OpCodes.Stloc, s0);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Ldloc, s0);
            pILGet.EmitCall(OpCodes.Call, prop.GetSetMethod(), null);
            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldloc, s0);
            pILGet.Emit(OpCodes.Stloc, s2);
            pILGet.Emit(OpCodes.Br_S, label_IL_001e);
            pILGet.MarkLabel(label_IL_001e);
            pILGet.Emit(OpCodes.Ldloc, s2);
            pILGet.Emit(OpCodes.Ret);
#endif

            /*

            var s0 = pILGet.DeclareLocal(prop.PropertyType);
            var s1 = pILGet.DeclareLocal(typeof(Int32));
            var s2 = pILGet.DeclareLocal(prop.PropertyType);

            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.EmitCall(OpCodes.Call, prop.GetGetMethod(), null);
            pILGet.Emit(OpCodes.Stloc, s0);
            pILGet.Emit(OpCodes.Ldloc, s0);
            pILGet.Emit(OpCodes.Ldnull);
            pILGet.Emit(OpCodes.Cgt_Un);
            pILGet.Emit(OpCodes.Stloc, s1);
            pILGet.Emit(OpCodes.Ldloc, s1);
            Label label_IL_0015 = pILGet.DefineLabel();
            pILGet.Emit(OpCodes.Brfalse_S, label_IL_0015);
            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.Emit(OpCodes.Stloc, s2);
            Label label_IL_002a = pILGet.DefineLabel();
            pILGet.Emit(OpCodes.Br_S, label_IL_002a);
            pILGet.MarkLabel(label_IL_0015);
            var ctor = prop.PropertyType.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new Type[] { }, null);
            pILGet.Emit(OpCodes.Newobj, ctor);
            pILGet.EmitCall(OpCodes.Call, prop.GetSetMethod(), null);
            pILGet.Emit(OpCodes.Nop);
            pILGet.Emit(OpCodes.Ldarg_0);
            pILGet.EmitCall(OpCodes.Call, prop.GetGetMethod(), null);
            pILGet.Emit(OpCodes.Stloc, s2);
            pILGet.Emit(OpCodes.Br_S, label_IL_002a);
            pILGet.MarkLabel(label_IL_002a);
            pILGet.Emit(OpCodes.Ldloc, s2);
            pILGet.Emit(OpCodes.Ret);
           
            */
            /*

            LocalBuilder msgLocal = pILGet.DeclareLocal(typeof(string));//stringの変数を宣言する
            // msgLocal.SetLocalSymInfo("letter");//変数名を決める
            pILGet.Emit(OpCodes.Ldstr, "Hello World");//スタックにHello Worldをプッシュする(文字列の時)
            pILGet.Emit(OpCodes.Stloc, msgLocal);//スタックの一番上から現在の値をポップし変数に格納する
            pILGet.Emit(OpCodes.Ldloc, msgLocal);//スタックに変数からプッシュする
            Type tc = Type.GetType(typeof(Console).AssemblyQualifiedName);
            MethodInfo mi = tc.GetMethod("WriteLine", new Type[] { typeof(string) });
            pILGet.EmitCall(OpCodes.Call, mi, null);//呼び出す
            pILGet.Emit(OpCodes.Ret);

            //The proxy object
                       pILGet.Emit(OpCodes.Ldarg_0);
                        //The database
                        pILGet.Emit(OpCodes.Ldfld, database);
                        //The proxy object
                        pILGet.Emit(OpCodes.Ldarg_0);
                        //The ObjectId to look for
                        pILGet.Emit(OpCodes.Ldfld, f);
                        pILGet.Emit(OpCodes.Callvirt, typeof(MongoDatabase).GetMethod("Find", BindingFlags.Public | BindingFlags.Instance, null, new Type[] { typeof(ObjectId) }, null).MakeGenericMethod(info.PropertyType));
                        pILGet.Emit(OpCodes.Ret);*/

            PropertyBuilder newProp = typeBuilder.DefineProperty(prop.Name, PropertyAttributes.None, prop.PropertyType, Type.EmptyTypes);
            // newProp.SetCustomAttribute(new CustomAttributeBuilder(typeof(AttributeToBeAdded).GetConstructor(Type.EmptyTypes), Type.EmptyTypes, new FieldInfo[0], new object[0]));

            newProp.SetGetMethod(pGet);



            /*ConstructorBuilder staticConstructorBuilder = typeBuilder.DefineConstructor(MethodAttributes.Public | MethodAttributes.Static, CallingConventions.Standard, Type.EmptyTypes);
            ILGenerator staticConstructorILGenerator = staticConstructorBuilder.GetILGenerator();

            staticConstructorILGenerator.Emit(OpCodes.Ldobj, propLoader);
            staticConstructorILGenerator.Emit(OpCodes.Stsfld, fieldBuilder);*/

            var returnType = typeBuilder.CreateType();
            var f = returnType.GetField("__PlayerLoader");
            f.SetValue(null, propLoader);
            return returnType;
        }

    }
}