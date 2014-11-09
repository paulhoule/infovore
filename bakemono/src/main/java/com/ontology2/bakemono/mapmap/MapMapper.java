package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.abstractions.Codec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class MapMapper<InnerType,OutKey,OutValue>
        extends Mapper<LongWritable,Text,OutKey,OutValue> {

    // converting from Text to String is lame, but so what

    abstract Codec<InnerType> getCodec();
    abstract Function<InnerType,OutKey> getKeyFunction();
    abstract Function<InnerType,OutValue> getValueFunction();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InnerType row=getCodec().decode(value.toString());
        OutKey outKey=getKeyFunction().apply(row);
        OutValue outValue=getValueFunction().apply(row);

        if(outKey!=null && outValue!=null) {
            context.write(
                    outKey
                    ,outValue
            );
        }
    }
}
