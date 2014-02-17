package com.ontology2.bakemono.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.Arrays;

public class BloomReducer extends Reducer<Text,Writable,NullWritable,BloomFilter> {
    BloomFilter f;

    static final public String THIS="com.ontology2.bakemono.bloom.BloomReducer";
    static final public String VECTOR_SIZE=THIS+".vectorSize";
    static final public String NB_HASH=THIS+".nbHash";
    static final public String HASH_TYPE=THIS+".hashType";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration c=context.getConfiguration();
        int vectorSize=c.getInt(VECTOR_SIZE,0);
        int nbHash=c.getInt(NB_HASH,0);
        String hashType=c.get(HASH_TYPE, "murmur");
        f=new BloomFilter(vectorSize,nbHash, Hash.parseHashType(hashType));
    }

    @Override
    protected void reduce(Text key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
        f.add(toKey(key));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),f);
    }

    public static Key toKey(Text t) {
        return new Key(Arrays.copyOfRange(t.getBytes(), 0, t.getLength()));
    }

    public static Key toKey(String s) {
        return toKey(new Text(s));
    }
}
