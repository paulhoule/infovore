package com.ontology2.bakemono.rewriteSubject;

import com.ontology2.bakemono.joins.GeneralTextJoinMapper;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

import static com.google.common.collect.Maps.immutableEntry;

public class RewriteSubjectMapper extends GeneralTextJoinMapper {
    // In this case we are getting triples on the left and triples on the right and we are always
    // issuing the subject field as a key so it is very simple
    PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();

    @Override
    public Map.Entry<Text, Text> splitValue(Writable value, VIntWritable tag) {
        PrimitiveTriple t=ptc.decode(value.toString());
        return immutableEntry(new Text(t.getSubject()), (Text) value);
    }
}
