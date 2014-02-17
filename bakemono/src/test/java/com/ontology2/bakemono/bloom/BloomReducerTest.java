package com.ontology2.bakemono.bloom;

import com.ontology2.bakemono.RecyclingIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class BloomReducerTest {
    Reducer.Context context;
    BloomReducer reducer;

    @Before
    public void setup() throws IOException, InterruptedException {
        context=mock(Reducer.Context.class);
        Configuration c=new Configuration();
        c.set(BloomReducer.VECTOR_SIZE,"100000");
        c.set(BloomReducer.NB_HASH,"10");
        when(context.getConfiguration()).thenReturn(c);
        reducer=new BloomReducer();
        reducer.setup(context);
    }

    @Test
    public void tryItOut() throws IOException, InterruptedException {
        Iterable<Writable> recyclingIterable=new RecyclingIterable(
                LongWritable.class,
                new LongWritable(1)
        );

        reducer.reduce(new Text("New York"),recyclingIterable,context);
        reducer.reduce(new Text("New Jersey"),recyclingIterable,context);
        reducer.reduce(new Text("New Mexico"),recyclingIterable,context);
        reducer.reduce(new Text("New Hampshire"),recyclingIterable,context);

        reducer.reduce(new Text("Beyonce"),recyclingIterable,context);
        reducer.reduce(new Text("Gwen Stefani"),recyclingIterable,context);
        reducer.reduce(new Text("Lady Gaga"),recyclingIterable,context);
        reducer.reduce(new Text("Madonna"),recyclingIterable,context);

        reducer.cleanup(context);

        ArgumentCaptor<BloomFilter> argument = ArgumentCaptor.forClass(BloomFilter.class);
        verify(context).write(
                any(),
                argument.capture());

        BloomFilter f=argument.getValue();
        assertFalse(f.membershipTest(BloomReducer.toKey("Michigan")));
        assertTrue(f.membershipTest(BloomReducer.toKey("New Jersey")));
        assertTrue(f.membershipTest(BloomReducer.toKey("New Mexico")));
        assertTrue(f.membershipTest(BloomReducer.toKey("Lady Gaga")));
        assertTrue(f.membershipTest(BloomReducer.toKey("Beyonce")));
        assertFalse(f.membershipTest(BloomReducer.toKey("Olivia Newton-John")));
    }

    @Test
    public void justBloom() {
        BloomFilter f=new BloomFilter(100000,10, Hash.parseHashType("murmur"));
        f.add(new Key(new Text("New Jersey").getBytes()));
        assertTrue(f.membershipTest(new Key(new Text("New Jersey").getBytes())));
    }
}
