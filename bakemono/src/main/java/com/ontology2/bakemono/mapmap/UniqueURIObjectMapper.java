package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.ProjectURIObject;
import org.apache.hadoop.io.Text;

public class UniqueURIObjectMapper extends PTUniqueMapMapper<Text> {

    private final Function<PrimitiveTriple,Text> primitiveTriple = new ProjectURIObject();

    @Override
    Function<PrimitiveTriple, Text> getKeyFunction() {
        return primitiveTriple;
    }

}
