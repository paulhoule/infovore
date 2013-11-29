package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.abstractions.Codec;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.LongWritable;

import javax.annotation.Nullable;

public abstract class PTUniqueMapMapper<OutKey>
        extends MapMapper<PrimitiveTriple,OutKey,LongWritable> {

    private final Codec<PrimitiveTriple> codec=
            new PrimitiveTripleCodec();

    private final Function<PrimitiveTriple,LongWritable> valueFunction=
            new Function<PrimitiveTriple,LongWritable>() {
                @Nullable
                @Override
                public LongWritable apply(@Nullable PrimitiveTriple primitiveTriple) {
                    return ONE;
                }
            };

    private final LongWritable ONE=new LongWritable(1);

    @Override
    Codec<PrimitiveTriple> getCodec() {
        return codec;
    };

    @Override
    Function<PrimitiveTriple,LongWritable> getValueFunction() {
        return valueFunction;
    }

}
