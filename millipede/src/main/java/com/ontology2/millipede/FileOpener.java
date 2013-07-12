package com.ontology2.millipede;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

//
// Automatically handles file compression and charsets
//

public class FileOpener {
	
	private static final String BZIP2 = "pbzip2";

	public BufferedReader createBufferedReader(String filename) throws Exception {
		return new BufferedReader(createReader(filename));
	}
	
	public Reader createReader(String filename) throws Exception {
		return new InputStreamReader(createInputStream(filename),"UTF-8");
	}
	
	public PrintWriter createWriter(String filename) throws Exception {
		return new PrintWriter(new OutputStreamWriter(createOutputStream(filename),"UTF-8"));
	}
	
	public OutputStream createOutputStream(String filename) throws IOException {
		OutputStream stream;
		Files.createParentDirs(new File(filename));
		return new FileOutputStream(filename);
	}

	public InputStream createInputStream(String filename) throws Exception {
		InputStream stream;
		if (filename.endsWith(".gz")) {
			InputStream innerStream=new FileInputStream(filename);
			stream=new GZIPInputStream(innerStream);
		} else if (filename.endsWith(".bz2")) {
			stream=decompressWithExternalBzip(filename);
		} else {
			stream=new FileInputStream(filename);
		}
		
		return stream;
	}
	
	public InputStream decompressWithExternalBzip(String filename) throws Exception {
		for(String bzcatPath:searchWidelyForCommand(BZIP2)) {
			if (new File(bzcatPath).canExecute()) {
				Process p=new ProcessBuilder(new String[] {
						bzcatPath,
						"-c",
						"-d",
						"-"
				})
				.redirectInput(new File(filename))
				.start();
				
				return p.getInputStream();
			}
		}
		
		throw new Exception("Could not find a working copy of bzip");
	}

	public OutputStream compressWithExternalBzip(String filename) throws Exception {
		for(String bzipPath:searchWidelyForCommand(BZIP2)) {
			if (new File(bzipPath).canExecute()) {
				final Process p=new ProcessBuilder(new String[] {
						bzipPath,
						"-c",
						"-"
				}).redirectOutput(new File(filename))
				.redirectInput(Redirect.PIPE)
				.start();
				
				return new FilterOutputStream(p.getOutputStream()) {

					@Override
					public void close() throws IOException {
						super.close();
						try {
							p.waitFor();
						} catch (InterruptedException e) {
							new RuntimeException(e);
						}

					}
				};
			}
		}
		
		throw new Exception("Could not find a working copy of bzip");
	}

	public List<String> searchWidelyForCommand(String cmd) {
		List<String> paths=Lists.newArrayList();
		paths.add("c:/cygwin/bin/"+cmd+".exe");
		String path=System.getenv("PATH");
		Iterable<String> parts=Splitter.on(System.getProperty("path.separator")).split(path);
		for(String dir:parts) {
			paths.add(dir+"/"+cmd);
			paths.add(dir+"/"+cmd+".exe");
		}
		
		return paths;
	}
	
	public ObjectOutputStream createObjectOutputStream(String filename) throws IOException {
		return new ObjectOutputStream(createOutputStream(filename));
	}

	public void writeObject(String file, Serializable that) throws Exception {
		ObjectOutputStream oos=new ObjectOutputStream(createOutputStream(file));
		oos.writeObject(that);
		oos.close();
		
	}

	public <T extends Serializable> T readObject(String file) throws Exception {
		ObjectInputStream ois=new ObjectInputStream(createInputStream(file));
		try {
			return (T) ois.readObject();
		} finally {
			ois.close();
		}
	}
}
