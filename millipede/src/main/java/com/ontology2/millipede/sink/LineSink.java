package com.ontology2.millipede.sink;

import java.io.PrintWriter;

import com.hp.hpl.jena.rdf.model.Model;


public class LineSink extends EmptyReportSink<String> implements Sink<String> {

    private PrintWriter writer;

    public LineSink(PrintWriter writer) {
        this.writer=writer;
    }

    @Override
    public void accept(String obj) throws Exception {
        writer.println(obj);

    }

    @Override
    public Model close() throws Exception {
        writer.close();
        return super.close();
    }

}
