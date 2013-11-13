package com.ontology2.bakemono.joins;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class TaggedKey<T extends WritableComparable> implements WritableComparable {

    //
    // Thanks to type erasure,  we've got to override this to provide a constructor
    // for a blank key
    //

    protected  abstract  T newT();

    private T key;
    private VIntWritable tag;

    public TaggedKey() {};
    public TaggedKey(T key,VIntWritable tag) {
        this.key=key;
        this.tag=tag;
    }

    public T getKey() {
        return key;
    }
    public VIntWritable getTag() {
        return tag;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        key.write(d);
        tag.write(d);
    }

    @Override
    public void readFields(DataInput d) throws IOException {
        if(key==null) {
            key=newT();
        }

        if(tag==null) {
            tag=new VIntWritable();
        }

        key.readFields(d);
        tag.readFields(d);
    }

    @Override
    public int compareTo(Object o) {
        TaggedKey that=(TaggedKey) o;
        int cmp=key.compareTo(that.key);
        return cmp==0 ? tag.compareTo(that.tag) : cmp;
    }

    @Override
    public boolean equals(Object o) {
        TaggedKey that=(TaggedKey) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }


}
