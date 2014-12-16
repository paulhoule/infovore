package com.ontology2.bakemono.pse3;

import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.abstractions.PrimaryKeyValueAcceptor;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTripleCodec;
import com.ontology2.rdf.InvalidNodeException;
import com.ontology2.rdf.JenaUtil;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class PSE3Mapper extends Mapper<LongWritable,Text,WritableTriple,WritableTriple> {
    private static final LongWritable ONE = new LongWritable(1);
    
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(PSE3Mapper.class);
    final LoadingCache<String,Node> nodeParser=JenaUtil.createNodeParseCache();

    final static PrimitiveTripleCodec p3Codec=new PrimitiveTripleCodec();
    private final Pattern $escape=Pattern.compile("[$][0-9A-F]{4}");
    
    //
    // all of these are deliberately in the default scope so that the test classes
    // can mess with them
    //

    KeyValueAcceptor<WritableTriple,WritableTriple> accepted;
    
    @Override
    public void setup(Context context) throws IOException,
    InterruptedException {
        super.setup(context);
        accepted=new PrimaryKeyValueAcceptor(context);
    }

    Function<String,String> nodePreprocessor=new Unescape$();
    int myCnt=0;
    WritableTriple writableTriple;
    @Override
    public void map(LongWritable arg0, Text arg1, Context c) throws IOException, InterruptedException {
        PrimitiveTriple row3=p3Codec.decode(arg1.toString());
        try {
            String rawSubject = nodePreprocessor.apply(row3.getSubject());
            String rawPredicate = nodePreprocessor.apply(row3.getPredicate());
            String rawObject = nodePreprocessor.apply(row3.getObject());
            
            Node_URI subject=(Node_URI) nodeParser.get(rawSubject);
            Node_URI predicate=(Node_URI) nodeParser.get(rawPredicate);
            Node object=nodeParser.get(rawObject);
            object=postprocessObject(object);
            Triple realTriple=new Triple(subject,predicate,object);

            writableTriple = new WritableTriple(realTriple);
        } catch(Throwable e) {
            String factString=row3.getSubject()+"\t"+row3.getPredicate()+"\t"+row3.getSubject()+"\t.";
            logger.warn("Caught exception while parsing fact ["+factString+"]",e);
            reject(c, row3);
            return;
        }
        accepted.write(writableTriple,writableTriple,c);
        incrementCounter(c,PSE3Counters.ACCEPTED,1);
    }

    private Node postprocessObject(Node object) throws InvalidNodeException {
        if(object.isLiteral()) {
            String value = object.getLiteralLexicalForm();
            if (object.getLiteralDatatype() == XSDDatatype.XSDdateTime) {
;
                if (!PSE3Util.dateTimePattern().matcher(value).matches()) {
                    throw new InvalidNodeException(value + " is not a valid xsd:dateTime");
                }
            }
            if (object.getLiteralLexicalForm().length() > MAX_STRING_LENGTH)
                throw new InvalidNodeException(value + " exceedes the maximum string length of " + MAX_STRING_LENGTH);
        }

        return object;
    }

    public static final int MAX_STRING_LENGTH=63999;

    //
    // Barf on $xxxx escape sequences in any data type
    //
    
    private boolean has$escape(Node that) {
        return $escape.matcher(that.toString()).find();
    }

    private void reject(Context c, PrimitiveTriple row3) throws IOException,
            InterruptedException {
        incrementCounter(c,PSE3Counters.REJECTED,1);
    }

    //
    // this code prevents failing test because the mock object Context we are passing back
    // always returns null from getCounter...  With a more sophisticated mock object perhaps
    // the system will produce individual mocks for each counter so we can watch what
    // happens with counters
    //
    
    private void incrementCounter(Context context,Enum <?> counterId,long amount) {
        Counter counter=context.getCounter(counterId);
        if(counter!=null) {
            counter.increment(amount);
        };
    };

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
    }
    
    public class Unescape$ implements Function<String,String>{

        @Override
        public String apply(String input) {
            if(input.startsWith("<") && input.endsWith(">"))
                return applyToNode(input);
            
            if(input.startsWith("\"") && input.endsWith("\""))
                return applyToRawString(input);
            
            return input;
        }
        
        public String applyToNode(String input) {
            return unescapeFreebaseKey(input);
        }

        // XXX -- note that this is "not implemented",  is this what we want?

        public String applyToRawString(String input) {
            return input;
        }
    }

    // would L.U.T. be faster?
    public static int digitToHex(char digit) {
        if(digit<='9' && digit>='0') {
            return digit-'0';
        }

        if(digit<='F' && digit>='A') {
            return digit-'A'+10;
        }

        return -1;
    }

    public static String unescapeFreebaseKey(String in) {
        int from=0;
        int to=in.indexOf('$');
        if(to==-1)
            return in;


        StringBuilder out=new StringBuilder();
        do {
            out.append(in.substring(from,to));
            if(in.length()<to+5)
                return in;

            int a=digitToHex(in.charAt(to+1));
            int b=digitToHex(in.charAt(to+2));
            int c=digitToHex(in.charAt(to+3));
            int d=digitToHex(in.charAt(to+4));
            if (a!=-1 && b!=-1 && c!=-1 && d!=-1) {
                out.append((char) ((a << 12) + (b << 8) + (c << 4) + d));
            } else {
                return in;
            }
            from=to+5;
            to=in.indexOf('$',to+5);
        } while(to!=-1);

        if(from<in.length()) {
            out.append(in.substring(from));
        }

        return out.toString();
    }
}
