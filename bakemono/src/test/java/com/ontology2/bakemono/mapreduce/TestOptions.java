package com.ontology2.bakemono.mapreduce;

import com.ontology2.centipede.parser.HasOptions;

import static com.google.common.collect.Lists.*;

public class TestOptions implements HasOptions {
    public Iterable<String> input=newArrayList("/are","/friends","/electric");
    public String output="/horseheads";
    public int reducerCount=33;
}
