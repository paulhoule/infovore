package com.ontology2.hydroxide.shell;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.shell.MillipedeShell;

public class InfovoreShell extends MillipedeShell{
	public InfovoreShell(String[] arguments) {
		super(arguments);
	}

	@Override
	public String getShellName() {
		return "infovore";
	}

	public static void main(String[] args) throws IOException {
		new MillipedeShell(args).run();
	}
}
