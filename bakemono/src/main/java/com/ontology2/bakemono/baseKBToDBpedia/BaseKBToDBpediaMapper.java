package com.ontology2.bakemono.baseKBToDBpedia;

import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.ontology2.bakemono.util.StatelessIdFunctions.dbpediaEscape;
import static com.ontology2.bakemono.util.StatelessIdFunctions.unescapeKey;

public class BaseKBToDBpediaMapper extends Mapper<LongWritable,Text,Text,Text> {
    final PrimitiveTripleCodec codec=new PrimitiveTripleCodec();

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        PrimitiveTriple pt=codec.decode(value.toString());
        final String prefix = "/wikipedia/en_title/";

        if(pt.getPredicate().equals("<http://rdf.basekb.com/ns/type.object.key>")) {
            String fbKey=pt.getObject();
            if (fbKey.startsWith("\"") || fbKey.endsWith("\"")) {
                fbKey=fbKey.substring(1,fbKey.length()-1);

                if(fbKey.startsWith(prefix)) {
                    String wikiKey=fbKey.substring(prefix.length());
                    String dbpediaURI="http://dbpedia.org/resource/"+mapKey(wikiKey);
                    context.write(
                            new Text(pt.getSubject()),
                            new Text("<http://www.w3.org/2002/07/owl#sameAs>\t<"+dbpediaURI+">\t.")
                    );
                }
            }
        }
    }

    public static String mapKey(String wikiKey) {
        return dbpediaEscape(unescapeKey(wikiKey));
    }
}
