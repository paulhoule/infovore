package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.ProjectInternalURIObject;
import org.apache.hadoop.io.Text;

public class UniqueInternalURIObjectMapper extends PTUniqueMapMapper<Text> {

    private final Function<PrimitiveTriple,Text> primitiveTriple = new ProjectInternalURIObject();

    @Override
    Function<PrimitiveTriple, Text> getKeyFunction() {
        return primitiveTriple;
    }

}