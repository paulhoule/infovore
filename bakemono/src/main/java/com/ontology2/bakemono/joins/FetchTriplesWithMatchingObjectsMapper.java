package com.ontology2.bakemono.joins;

import com.google.common.collect.Maps;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class FetchTriplesWithMatchingObjectsMapper extends GeneralTextJoinMapper {

    private static final Text EMPTY =new Text("");
    private static final PrimitiveTripleCodec ptc=new PrimitiveTripleCodec();

    @Override
    public Map.Entry<Text, Text> splitValue(Writable value, VIntWritable tag) {
        switch(tag.get()) {
            case 1:
                return Maps.immutableEntry((Text) value, EMPTY);
            case 2:
                PrimitiveTriple triple=ptc.decode(value.toString());
                return Maps.immutableEntry(new Text(triple.getObject()),(Text) value);
        }

        return Maps.immutableEntry((Text) value, (Text) value);
    }
}
