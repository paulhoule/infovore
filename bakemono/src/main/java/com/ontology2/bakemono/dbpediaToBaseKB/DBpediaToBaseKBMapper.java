package com.ontology2.bakemono.dbpediaToBaseKB;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import static com.ontology2.bakemono.util.StatelessIdFunctions.*;

public class DBpediaToBaseKBMapper extends Mapper<LongWritable,Text,Text,Text> {
    final PrimitiveTripleCodec codec=new PrimitiveTripleCodec();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    //
    // XXX -- mapper doesn't really work just yet!
    //

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PrimitiveTriple pt=codec.decode(value.toString());
        if(pt.getPredicate().equals("<http://rdf.basekb.com/ns/type.object.key>")) {
            String fbKey=pt.getObject();
            if(fbKey.startsWith("/wikipedia/en/")) {
                String wikiKey=fbKey.substring("/wikipedia/en".length());
                String dbpediaURI="http://dbpedia.org/resource/"+mapKey(wikiKey);
            }
        }
    }

    //
    // the purpose of this function is to take a key encoded in Freebase style and re-encode it
    // DBpedia style
    //
    public static String mapKey(String wikiKey) {
        return dbpediaEscape(unescapeKey(wikiKey));
    }

}
