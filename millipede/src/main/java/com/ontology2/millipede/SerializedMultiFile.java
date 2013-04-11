package com.ontology2.millipede;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.ontology2.millipede.sink.Sink;

public class SerializedMultiFile<T extends Serializable> extends MultiFile<T> {

	public SerializedMultiFile(String directory, String nameBase,
			String nameExtension, PartitionFunction<T> f) {
		super(directory, nameBase, nameExtension, f);
	}

	public T readFirstObject(int binNumber) throws Exception, Exception {
		ObjectInputStream input=new ObjectInputStream(createInputStream(binNumber));
		try {
			return (T) input.readObject();
		} finally {
			input.close();
		}
	}
	
	@Override
	public long pushBin(int binNumber, Sink<T> destination) throws Exception {
		int count=0;
		ObjectInputStream input=new ObjectInputStream(createInputStream(binNumber));
		try {
			while(true) {
				destination.accept((T) input.readObject());
				count++;
			}
		} catch(EOFException eofEx) {
		} finally {
			input.close();
		}
		
		return count;
	}

	@Override
	public Sink<T> createSink(int binNumber) throws Exception {
		final ObjectOutputStream output=new ObjectOutputStream(createOutputStream(binNumber));
		return new Sink<T>() {

			@Override
			public void accept(T obj) throws Exception {
				output.writeObject(obj);
			}

			@Override
			public void close() throws Exception {
				output.close();
			}
		};
	}

}
