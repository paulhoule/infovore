package com.ontology2.bakemono.entityCentric;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.io.IOException;

public class EntityCentricMapper extends Mapper<LongWritable,Text,Text,Text> {
    static final PrimitiveTripleCodec codec=new PrimitiveTripleCodec();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PrimitiveTriple t=codec.decode(value.toString());
        context.write(new Text(t.getSubject()),value);
    }
}
