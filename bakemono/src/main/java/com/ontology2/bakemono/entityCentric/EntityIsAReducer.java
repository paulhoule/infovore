package com.ontology2.bakemono.entityCentric;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

public class EntityIsAReducer extends EntityMatchesRuleReducer<Text,Text> {
    Set<String> typeList;

    final static PrimitiveTripleCodec codec=new PrimitiveTripleCodec();
    final static String A="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    public static final String IS_A="com.ontology2.bakemono.entityCentric.EntityIsAReducer";
    public static final String TYPE_LIST=IS_A+".typeList";
    static Logger log= Logger.getLogger(EntityIsAReducer.class);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration that=context.getConfiguration();
        typeList= Sets.newHashSet(Splitter.on(",").split(that.get(TYPE_LIST)));
        log.info("Initializing type list");
        for(String type:typeList)
            log.info("Accepting type: ["+type+"]");
    }

    @Override
    protected Text copy(Text text) {
        return new Text(text);
    }

    @Override
    protected boolean matches(Text subject, Iterable<Text> facts) {
        for(Text fact:facts) {
            PrimitiveTriple pt=codec.decode(fact.toString());
            if (A.equals(pt.getPredicate()) && typeList.contains(pt.getObject()) ) {
                return true;
            }
        }
        return false;
    }
}
