package com.ontology2.bakemono.freebasePrefilter;

import com.google.common.base.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ontology2.bakemono.abstractions.Codec;
import com.ontology2.bakemono.primitiveTriples.*;
import com.ontology2.rdf.InvalidNodeException;
import com.ontology2.rdf.InvalidPrefixException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Functions.*;

public class FreebaseRDFMapper extends Mapper<LongWritable,Text,Text,Text> {
    private static org.apache.commons.logging.Log logger = LogFactory.getLog(FreebaseRDFMapper.class);
    ImmutableMap.Builder<String,String> prefixBuilder=new ImmutableMap.Builder<String,String>();
    ImmutableMap<String,String> prefixMap = ImmutableMap.of();
    Codec<PrimitiveTriple> ptCodec=new PrimitiveTripleCodec();
    private Predicate<PrimitiveTriple> tripleFilter;
    private Function<PrimitiveTriple, PrimitiveTriple> rewritingFunction;

    public void declarePrefix(String obj) {
        if(obj.startsWith("@prefix")) {
            try {
                List<String> parts=splitPrefixDeclaration(obj);
                if(!prefixMap.containsKey(parts.get(1))) {
                    prefixBuilder.put(parts.get(1),parts.get(2));
                    prefixMap=prefixBuilder.build();
                }
            } catch(InvalidPrefixException ex) {
                logger.warn("Invalid prefix declaration: "+obj);
                return;
            }
        }
    }


    @Override
    public void setup(Context job) {
        declarePrefix("@prefix ns: <http://rdf.freebase.com/ns/>.");
        declarePrefix("@prefix key: <http://rdf.freebase.com/key/>.");
        declarePrefix("@prefix owl: <http://www.w3.org/2002/07/owl#>.");
        declarePrefix("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
        declarePrefix("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");
        declarePrefix("@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.");		


        tripleFilter=acceptTheseTriples();
        rewritingFunction=tripleRewritingFunction();

    }

    final static Splitter lineSplitter = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings().limit(3);
    final static Splitter iriSplitter = Splitter.on(":").limit(2);

    @Override
    public void map(LongWritable k, Text v,Context c) throws IOException, InterruptedException {

        String line=v.toString();
        if (line.startsWith("@prefix")) {
            incrementCounter(c,FreebasePrefilterCounter.PREFIX_DECL,1L);
            return;
        }

        try {
            List<String> parts = expandTripleParts(line);
            line.getBytes();
            PrimitiveTriple triple=new PrimitiveTriple(parts.get(0),parts.get(1),parts.get(2));


            if(tripleFilter.apply(triple)) {
                triple=rewritingFunction.apply(triple);
                accept(c,triple);
                incrementCounter(c,FreebasePrefilterCounter.ACCEPTED,1L);
            } else {
                incrementCounter(c,FreebasePrefilterCounter.IGNORED,1L);
            }

        } catch(InvalidNodeException ex) {
            incrementCounter(c,FreebasePrefilterCounter.IGNORED,1L);
            logger.warn("Invalid triple: "+line);
        }

        return;				
    }

    private void incrementCounter(Context context,Enum <?> counterId,long amount) {
        Counter counter=context.getCounter(counterId);
        if(counter!=null) {
            counter.increment(amount);
        };
    };

    private void accept(Context out,
            PrimitiveTriple primitiveTriple) throws IOException, InterruptedException {
        out.write(new Text(primitiveTriple.getSubject()), new Text(primitiveTriple.poPairAsString()));
    }

    List<String> expandTripleParts(String line) throws InvalidNodeException {
        List<String> parts=splitTriple(line);

        parts.set(0,rewriteNode(expandIRINode(parts.get(0))));
        parts.set(1,rewriteNode(expandIRINode(parts.get(1))));
        parts.set(2,rewriteNode(expandAnyNode(parts.get(2).trim())));
        return parts;
    }

    static List<String> splitTriple(String obj) throws InvalidNodeException {
        if (!obj.endsWith(".")) {
            throw new InvalidNodeException();
        }

        obj=obj.substring(0,obj.length()-1);
        List<String> parts=Lists.newArrayList(lineSplitter.split(obj));
        if (parts.size()!=3) {
            throw new InvalidNodeException();
        }
        return parts;
    }

    public String expandIRINode(String string) throws InvalidNodeException {
        List<String> parts=Lists.newArrayList(iriSplitter.split(string));
        
        if (string.startsWith("<") && string.endsWith(">")) {
            return string;
        }
        
        if (prefixMap.containsKey(parts.get(0))) {
            return "<"+prefixMap.get(parts.get(0))+parts.get(1)+">";
        }
        
      
        throw new InvalidNodeException();
    }

    public String expandAnyNode(String string) {
        List<String> parts=Lists.newArrayList(iriSplitter.split(string));
        
        if (string.startsWith("<") && string.endsWith(">")) {
            return string;
        }
        if (prefixMap.containsKey(parts.get(0))) {
            return "<"+prefixMap.get(parts.get(0))+parts.get(1)+">";
        }

        return string;
    }

    public String rewriteNode(String uri) {
        if(!uri.startsWith("<") && uri.endsWith(">")) {
            return uri;
        }
        
        if(uri.startsWith("<http://rdf.freebase.com/")) {
            uri="<http://rdf.basekb.com/"+uri.substring("<http://rdf.freebase.com/".length());
        }
        
        return uri;
    };
    
    public static List<String> splitPrefixDeclaration(String obj) throws InvalidPrefixException {
        List<String> parts=Lists.newArrayList(Splitter.on(" ").split(obj));
        if (parts.size()!=3) {
            throw new InvalidPrefixException();
        }

        String prefix=parts.get(1);
        String mapsTo=parts.get(2);	

        if (!prefix.endsWith(":")) {
            throw new InvalidPrefixException();
        }

        parts.set(1, prefix.substring(0, prefix.length()-1));

        if (!mapsTo.startsWith("<") || !mapsTo.endsWith(">.")) {
            throw new InvalidPrefixException();
        }

        parts.set(2, mapsTo.substring(1, mapsTo.length()-2));

        return parts;
    } 

    public static Predicate <PrimitiveTriple> acceptTheseTriples() {
        return Predicates.not(Predicates.or(
                PrimitiveTriple.hasPredicate("<http://rdf.basekb.com/ns/type.type.instance>"),
                PrimitiveTriple.hasPredicate("<http://rdf.basekb.com/ns/type.type.expected_by>"),
                PrimitiveTriple.hasPredicate("<http://rdf.basekb.com/ns/common.notable_for.display_name>"),
                Predicates.and(
                        PrimitiveTriple.hasPredicate("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                        PrimitiveTriple.objectMatchesPrefix("<http://rdf.basekb.com")
                        )
                ));
    }


    public static Function<PrimitiveTriple, PrimitiveTriple> tripleRewritingFunction() {
        return compose(compose(
                new PrimitiveTripleReverser(
                        "<http://rdf.basekb.com/ns/type.permission.controls>"
                        , "<http://rdf.basekb.com/ns/m.0j2r9sk>")
                , new PrimitiveTripleReverser(
                        "<http://rdf.basekb.com/ns/dataworld.gardening_hint.replaced_by>"
                        , "<http://rdf.basekb.com/ns/m.0j2r8t8>"))
                , new PrimitiveTriplePredicateRewriter(
                "<http://rdf.basekb.com/ns/type.object.type>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));

    }

}
