package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/12/13
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExtractInternalURIObject implements Function<PrimitiveTriple,Text> {
    @Override
    public Text apply(PrimitiveTriple t) {
        String o=t.getObject();
        if(o.startsWith("<http://rdf.basekb.com/") && o.endsWith(">")) {
            return new Text(t.getObject());
        }

        return null;
    }
}
