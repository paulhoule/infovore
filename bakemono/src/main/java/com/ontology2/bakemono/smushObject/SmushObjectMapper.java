package com.ontology2.bakemono.smushObject;

import com.ontology2.bakemono.joins.GeneralTextJoinMapper;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

import static com.google.common.collect.Maps.immutableEntry;

public class SmushObjectMapper extends GeneralTextJoinMapper {

    PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();
    @Override
    public Map.Entry<Text, Text> splitValue(Writable value, VIntWritable tag) {
        PrimitiveTriple t=ptc.decode(value.toString());
        if (tag.get()==1) {
            return immutableEntry(new Text(t.getSubject()), (Text) value);
        }
        return immutableEntry(new Text(t.getObject()), (Text) value);
    }
}
