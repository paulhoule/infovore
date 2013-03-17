package com.ontology2.hydroxide.shell;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import com.ontology2.millipede.shell.CommandLineApplication;
import com.ontology2.millipede.shell.MillipedeShell;

public class InfovoreShell extends MillipedeShell{

	@Override
	public String getShellName() {
		return "infovore";
	}
	
	@Override
	public List<String> getApplicationContextPath() {
		List<String> that=Lists.newArrayList("com/ontology2/millipede/shell/applicationContext.xml");
		that.add("com/ontology2/hydroxide/shell/applicationContext.xml");
		return that;
	}
	
	public static void main(String[] args) {
		new InfovoreShell().run(args);
	}
}
