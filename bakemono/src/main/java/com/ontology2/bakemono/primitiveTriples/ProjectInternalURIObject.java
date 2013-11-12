package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;
import org.apache.hadoop.io.Text;

public class ProjectInternalURIObject implements Function<PrimitiveTriple,Text> {
    @Override
    public Text apply(PrimitiveTriple t) {
        String o=t.getObject();
        if(o.startsWith("<http://rdf.basekb.com/") && o.endsWith(">")) {
            return new Text(t.getObject());
        }

        return null;
    }
}
