package com.ontology2.bakemono.pse3;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.jena.WritableTriple;

public class SchwarzeneggerTest {
    PSE3Mapper pse3mapper;
    private BufferedReader reader;

    @Before
    public void setup() {
        pse3mapper=new PSE3Mapper();
        pse3mapper.accepted=mock(KeyValueAcceptor.class);
        InputStream input=getClass().getResourceAsStream("arnold-10.nt");
        reader = new BufferedReader(new InputStreamReader(input,Charsets.UTF_8));
    }
    
    @Test
    public void tenInputs() throws IOException {
        int count=0;
        while(reader.readLine()!=null)
            count++;
        
        assertEquals(10,count);
    }
    
    @Test
    public void noDuplicates() throws IOException, InterruptedException {
        int count=1;
        String line;
        List<WritableTriple> outputs=Lists.newArrayList();
        
        while((line=reader.readLine())!=null) {
            LongWritable key=new LongWritable(count);
            Text value=new Text(line);
            Mapper.Context context = mock(Context.class);
            pse3mapper.map(key, value, context);
            ArgumentCaptor<WritableTriple> captorS=ArgumentCaptor.forClass(WritableTriple.class);
            ArgumentCaptor<WritableTriple> captorV=ArgumentCaptor.forClass(WritableTriple.class);
            verify(pse3mapper.accepted).write(
                    captorS.capture(),
                    captorV.capture(),
                    eq(context));
            outputs.add(captorS.getValue());
            assertEquals(captorS.getValue(),captorV.getValue());
            count++;
        }
        
        // no triple is equal to any other triple
        
        assertEquals(10,outputs.size());
        for(int i=0;i<outputs.size();i++) {
            for(int j=0;j<i;j++) {
                assertFalse(outputs.get(i).equals(outputs.get(j)));
            }
        }
        
        // each triple is equal to itself
        
        for(int i=0;i<outputs.size();i++) {
            assertTrue(outputs.get(i).equals(outputs.get(i)));
        }
        
        // the triple that disappears in the full system appears in the output
        
        WritableTriple croatianKey=new WritableTriple(new Triple(
                Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                ,Node_URI.createURI("http://rdf.basekb.com/ns/type.object.key")
                ,Node.createLiteral("/wikipedia/hr_title/Arnold_Schwarzenegger")
        ));
        
        assertTrue(outputs.contains(croatianKey));
        
        //
        // hash values are all unique (they don't need to be,  but a collision
        // should be unlikely.)
        //
        Set<Integer> hashValues=Sets.newHashSet();
        for(WritableTriple that:outputs)
            hashValues.add(that.hashCode());
        
        assertEquals(10,hashValues.size());
    }
}
