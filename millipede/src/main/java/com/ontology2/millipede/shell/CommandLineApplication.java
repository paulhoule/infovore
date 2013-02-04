package com.ontology2.millipede.shell;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class CommandLineApplication implements Runnable {
	final String[] arguments;
	private static Log logger = LogFactory.getLog(CommandLineApplication.class);
	
	public CommandLineApplication(String[] arguments) {
		this.arguments=arguments;
	}

	@Override
	public void run() {
		try {
			_run();
		} catch(Exception e) {
			logger.error(e);
		}
	}

	protected abstract void _run() throws Exception;
	
}
