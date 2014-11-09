package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.ProjectPredicate;
import org.apache.hadoop.io.Text;

public class UniqueURIPredicateMapper extends PTUniqueMapMapper<Text> {

    private final Function<PrimitiveTriple,Text> primitiveTriple = new ProjectPredicate();

    @Override
    Function<PrimitiveTriple, Text> getKeyFunction() {
        return primitiveTriple;
    }
}
