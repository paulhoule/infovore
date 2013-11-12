package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/12/13
 * Time: 1:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProjectSubject implements Function<PrimitiveTriple,Text> {
    @Override
    public Text apply(PrimitiveTriple t) {
            return new Text(t.getSubject());
    }
}
