package com.ontology2.bakemono.mapreduce;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ShadowedParameterizedType implements ParameterizedType {
    final ParameterizedType that;
    final Type[] arguments;

    public ShadowedParameterizedType(ParameterizedType that, Type[] arguments) {
        this.that=that;
        this.arguments=arguments;
    }

    public Type[] getActualTypeArguments() {
        return arguments;
    }

    public Type getOwnerType() {
        return that.getOwnerType();
    }

    public Type getRawType() {
        return that.getRawType();
    }
}
