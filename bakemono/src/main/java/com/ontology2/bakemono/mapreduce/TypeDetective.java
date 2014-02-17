package com.ontology2.bakemono.mapreduce;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class TypeDetective {
    //
    // Note that this doesn't scan interfaces,  so you can get the type of an ArrayList<X>
    // but not the type of a List<X>
    //


    public static Type[] sniffTypeParameters(Type that,Class targetClass) {
        return sniffTypeParameters(that,targetClass,null);
    }

    public static Type[] sniffTypeParameters(Type that,Class targetClass,Map<TypeVariable,Type> typeParameters) {
        if (typeParameters==null)
            typeParameters=newHashMap();
        else
            typeParameters=newHashMap(typeParameters);

        if(that==Object.class)
            return null;

        if(that instanceof ParameterizedType) {
            ParameterizedType type=(ParameterizedType) that;
            if (type.getRawType()==targetClass) {
                Type[] arguments=type.getActualTypeArguments();
                fillInTypeVariables(arguments,typeParameters);
                return arguments;
            } else if (type.getRawType() instanceof Class) {
                Class clazz=(Class) type.getRawType();
                TypeVariable[] variables=clazz.getTypeParameters();
                Type[] values=type.getActualTypeArguments();
                for(int i=0;i<values.length;i++) {
                    typeParameters.put(variables[i],values[i]);
                }
                return sniffTypeParameters(clazz.getGenericSuperclass(),targetClass,typeParameters);
            }
        }

        if(that==targetClass)
            throw new SelfAwareTool.NoGenericTypeInformationAvailable("I can't read the generic type parameter for ["+targetClass+"] unless you subclass it with a concrete class.");

        if(that instanceof Class) {
            Class type=(Class) that;
            Type uber=type.getGenericSuperclass();
            return sniffTypeParameters(uber,targetClass,typeParameters);
        }

        return null;
    }

    public static void fillInTypeVariables(Type[] thoseTypes,Map<TypeVariable,Type> typeParameters) {
        for(int i=0;i<thoseTypes.length;i++) {
            thoseTypes[i]=fillInTypeVariables(thoseTypes[i],typeParameters);
        }
    }

    public static Type fillInTypeVariables(Type that,Map<TypeVariable,Type> typeParameters) {
        if(that instanceof TypeVariable) {
            if(typeParameters.containsKey(that)) {
                return typeParameters.get(that);
            }
        }

        if(that instanceof ParameterizedType) {
            ParameterizedType pType=(ParameterizedType) that;
            Type[] arguments=pType.getActualTypeArguments();
            fillInTypeVariables(arguments,typeParameters);
            return new ShadowedParameterizedType(pType,arguments);
        }
        return that;
    }
}
