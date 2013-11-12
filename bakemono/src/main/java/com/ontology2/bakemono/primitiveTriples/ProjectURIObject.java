package com.ontology2.bakemono.primitiveTriples;

import com.google.common.base.Function;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/10/13
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */

public class ProjectURIObject implements Function<PrimitiveTriple,Text> {
    @Override
    public Text apply(PrimitiveTriple t) {
        String o=t.getObject();
        if(o.startsWith("<") && o.endsWith(">")) {
            return new Text(t.getObject());
        }

        return null;
    }
}
