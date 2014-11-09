package com.ontology2.bakemono.mapmap;

import com.google.common.base.Function;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.ProjectSubject;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/12/13
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniqueURISubjectMapper extends PTUniqueMapMapper<Text> {

private final Function<PrimitiveTriple,Text> primitiveTriple = new ProjectSubject();

@Override
    Function<PrimitiveTriple, Text> getKeyFunction() {
        return primitiveTriple;
    }
}
