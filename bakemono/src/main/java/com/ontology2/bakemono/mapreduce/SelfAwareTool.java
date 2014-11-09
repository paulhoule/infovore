package com.ontology2.bakemono.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.ontology2.bakemono.joins.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.BeanNameAware;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class SelfAwareTool<OptionsClass> extends SingleJobTool<OptionsClass> implements BeanNameAware {
    Log LOG= LogFactory.getLog(SelfAwareTool.class);
    String beanName;
    static final Function<String,Path> STRING2PATH=new Function<String,Path>() {
        @Nullable @Override
        public Path apply(@Nullable String input) {
            return new Path(input);
        }
    };


    public static <T> T readField(Object that,String name) {
        try {
            Field f=that.getClass().getField(name);
            return (T) f.get(that);
        } catch(NoSuchFieldException|IllegalAccessException ex) {
            return null;
        }
    };

    //
    // Try to instantiate this class without creating a subclass and something awful will
    // happen!
    //

    public SelfAwareTool() {
    }

    @Override
    protected String getName() {
        return beanName;
    }

    @Override
    protected Class<? extends Mapper> getMapperClass()  {
        String thisClass=getClass().getName();
        if(thisClass.endsWith("Tool")) {
            thisClass=thisClass.substring(0,thisClass.length()-4);
        }

        String tryMapper=thisClass+"Mapper";
        try {
            return (Class<? extends Mapper>) Class.forName(tryMapper);
        } catch(ClassNotFoundException x) {
            return null;
        }
    }

    @Override
    protected Class<? extends Reducer> getReducerClass()  {
        String thisClass=getClass().getName();
        if(thisClass.endsWith("Tool")) {
            thisClass=thisClass.substring(0,thisClass.length()-4);
        }

        String tryMapper=thisClass+"Reducer";
        try {
            return (Class<? extends Reducer>) Class.forName(tryMapper);
        } catch(ClassNotFoundException x) {
            return super.getReducerClass();         // necessary because some jobs don't have a reducer
        }
    }

    public Class<? extends Writable> getMapInputKeyClass() {
        Type[] parameters= TypeDetective.sniffTypeParameters(getMapperClass(), Mapper.class);
        return toWritableClass(parameters[0]);
    }

    public Class<? extends Writable> getMapInputValueClass() {
        Type[] parameters= TypeDetective.sniffTypeParameters(getMapperClass(), Mapper.class);
        return toWritableClass(parameters[1]);
    }

    @Override
    public Class<? extends Writable> getMapOutputKeyClass() {
        Type[] parameters= TypeDetective.sniffTypeParameters(getMapperClass(), Mapper.class);
        return toWritableClass(parameters[2]);
    }

    @Override
    public Class<? extends Writable> getMapOutputValueClass() {
        Type[] parameters= TypeDetective.sniffTypeParameters(getMapperClass(), Mapper.class);
        return toWritableClass(parameters[3]);
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        Type[] parameters= TypeDetective.sniffTypeParameters(getReducerClass(), Reducer.class);
        return toWritableClass(parameters[2]);
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        Class mapperClass=getReducerClass();
        Type[] parameters= TypeDetective.sniffTypeParameters(getReducerClass(), Reducer.class);
        return toWritableClass(parameters[3]);
    }

    public static Class toWritableClass(Type t) {
        if (t instanceof Class)
            return (Class) t;

        if (t instanceof ParameterizedType) {
            ParameterizedType pt=(ParameterizedType) t;
            // yeah yeah,  some day this gets generalized and spun out into it's own class
            // which can be wired up through Spring if we want to -- the gist of this is that
            // there is an "official" implementation of a particular concrete subclass for a
            // given generic type
            if(TaggedItem.class.equals(pt.getRawType())) {
                if(pt.getActualTypeArguments()[0].equals(Text.class))
                    return TaggedTextItem.class;
            }

            return (Class) pt.getRawType();
        }

        throw new RuntimeException("Can't identify type ["+t+"] as a class");
    }

    protected Multimap<Integer,Path> tagMap=HashMultimap.create();

    //
    // Note that this has the side effect of setting the tagMap
    //

    @Override
    public Iterable<Path> getInputPaths() {
        Map<Field,Integer> declaredInputPaths=searchForInputPaths(getOptionsClass());
        if(declaredInputPaths.size()>1) {
            List<Path> allPaths=newArrayList();
            tagMap=HashMultimap.create();
            for(Map.Entry<Field,Integer> pair:declaredInputPaths.entrySet()) {
                try {
                    Object o=pair.getKey().get(options);
                    if (o instanceof String) {
                        Path that=STRING2PATH.apply((String) o);
                        allPaths.add(that);
                        tagMap.put(pair.getValue(),that);
                    } else if(o instanceof Iterable) {
                        for(Path that:transform((Iterable<String>) o,STRING2PATH)) {
                            allPaths.add(that);
                            tagMap.put(pair.getValue(),that);
                        }
                    }
                } catch(IllegalAccessException iae) {
                    LOG.warn("Java access controls blocked access to @InputPath on field "+pair.getKey());
                }
            }

            return allPaths;
        }

        Iterable<String> s=readField(options,"input");
        if(s==null)
            return null;
        return transform(s, STRING2PATH);
    }

    @Override
    public Multimap<Integer,Path> getTagMap() {
        return tagMap;
    }

    @Override
    public int getNumReduceTasks() {
        Integer numReduceTasks=readField(options,"reducerCount");
        return numReduceTasks==null ? 0 : numReduceTasks;
    }

    @Override
    protected Path getOutputPath() {
        String s=readField(options,"output");
        if(s==null)
            return null;
        return STRING2PATH.apply(s);
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        Class inKey=getMapInputKeyClass();
        Class inValue=getMapInputValueClass();
        if ( inValue==Text.class) {
            if(inKey==LongWritable.class) {
                return TextInputFormat.class;
            } else if(inKey==Text.class) {
                return KeyValueTextInputFormat.class;
            }
        }

        return SequenceFileInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        Class outKey=getOutputKeyClass();
        Class outValue=getOutputValueClass();
        if (outKey==Text.class) {
            if (outValue==Text.class || outValue==NullWritable.class)
                return TextOutputFormat.class;
        } else if (outValue==Text.class && outKey==NullWritable.class)
            return TextOutputFormat.class;

        return SequenceFileOutputFormat.class;
    }

    @Override
    public Class getOptionsClass() {
        return (Class) (TypeDetective.sniffTypeParameters(getClass(), SelfAwareTool.class))[0];
    }

    @Override
    public void setBeanName(String s) {
        beanName=s;
    }

    public static class NoGenericTypeInformationAvailable extends IllegalArgumentException {
        public NoGenericTypeInformationAvailable() {
        }

        public NoGenericTypeInformationAvailable(String s) {
            super(s);
        }

        public NoGenericTypeInformationAvailable(String message, Throwable cause) {
            super(message, cause);
        }

        public NoGenericTypeInformationAvailable(Throwable cause) {
            super(cause);
        }
    }

    //
    // Note the following is not really correct,  but it's tricky to make sense of
    // what we're getting back from reflection.  In particular,  when we see the type
    // parameters of Mapper,  we see a type variable rather than the actual type.  I
    // think we could figure the type because the type variable is filled in down
    // the inheritence stack,  but this will get the app working for now
    //

    @Override
    protected Class<? extends RawComparator> getGroupingComparatorClass() {
        Class mapInput=getMapOutputKeyClass();
        if(TaggedItem.class.isAssignableFrom(mapInput)) {
            return TaggedTextKeyGroupComparator.class;
        }

        return super.getGroupingComparatorClass();
    }

    @Override
    protected Class<? extends Partitioner> getPartitionerClass() {
        Class mapInput=getMapOutputKeyClass();
        if(TaggedItem.class.isAssignableFrom(mapInput)) {
            return TaggedKeyPartitioner.class;
        }

        return super.getPartitionerClass();
    }

    @Override
    protected Class<? extends RawComparator> getSortComparatorClass() {
        Class mapInput=getMapOutputKeyClass();
        if(TaggedItem.class.isAssignableFrom(mapInput)) {
            return TaggedTextKeySortComparator.class;
        }

        return super.getGroupingComparatorClass();
    }

    public static Map<Field,Integer> searchForInputPaths(Class optionClass) {
        Map<Field,Integer> map=newHashMap();
        for(Field f:optionClass.getFields()) {
            InputPath p=f.getAnnotation(InputPath.class);
            if(p!=null)
                map.put(f,p.value());
        }

        return map;
    }
}