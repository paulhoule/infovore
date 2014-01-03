package com.ontology2.bakemono;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainBase implements Runnable {

    private static Log logger = LogFactory.getLog(Main.class);
    private final ClassPathXmlApplicationContext context;

    public static class IncorrectUsageException extends Exception {
        public IncorrectUsageException(String message) {
            super(message);
        }

    }

    Map<String, TopLevelCommand> myCommands=new HashMap<String,TopLevelCommand>() {{
        put("run",new RunATool());
        put("list",new ListTools());
    }};

    final List<String> args;

    public MainBase(String[] arg0) {
        args= Lists.newArrayList(arg0);
        context=new ClassPathXmlApplicationContext(getApplicationContextPath().toArray(new String[] {}));
    }

    public List<String> getApplicationContextPath() {
        return Lists.newArrayList("com/ontology2/bakemono/applicationContext.xml");
    }

    @Override
    public void run() {
        try {
            try {
                parseArguments();
                cmd.run();
            } catch(IncorrectUsageException ex) {
                usage(ex);
                return;
            }
        } catch(Exception ex) {
            logger.error("Uncaught exception in application",ex);
        };
    }

    void parseArguments() throws Exception {
        if(args.isEmpty())
            errorCausedByUser("you didn't specify any arguments");

        cmd = myCommands.get(args.get(0));

        if(cmd==null)
            errorCausedByUser("bakemono only supports the following commands: "+ Joiner.on(" ").join(myCommands.keySet()));

        if(args.size()<cmd.getMinimumArgumentCount()+1)
            errorCausedByUser("the "+args.get(0)+" command requires at least one argument,  the name of a tool");

        cmd.validateArguments();
    }



    protected String toolName;
    protected Tool tool;
    protected List<String> toolArgs;

    TopLevelCommand cmd;

    String getToolName() {
        return this.toolName;
    }

    String getTool() {
        return this.toolName;
    }

    List<String> getToolArgs() {
        return toolArgs;
    }

    public static IncorrectUsageException errorCausedByUser(String error) throws IncorrectUsageException {
        throw new IncorrectUsageException(error);
    }

    private void usage(IncorrectUsageException ex) {
        System.out.println("User error: "+ex.getMessage());
    }

    abstract class TopLevelCommand implements Runnable {
        int getMinimumArgumentCount() { return 0; }
        void validateArguments() throws Exception {};
    };

    public class RunATool extends TopLevelCommand {
        @Override
        public void run()  {
            try {
                ToolRunner.run(tool, toolArgs.toArray(new String[0]));
            } catch(Exception ex) {
                logger.error("Uncaught exception in application",ex);
            };
        }

        @Override public int getMinimumArgumentCount() { return 1; }
        void validateArguments() throws Exception {
            toolName=args.get(1);
            try {
                tool=context.getBean(toolName,Tool.class);
            } catch(NoSuchBeanDefinitionException |BeanNotOfRequiredTypeException x) {
                errorCausedByUser("you specified a tool ["+toolName+"] not supported by the bakemono system");
            }

            toolArgs=Lists.newArrayList(Iterables.skip(args, 2));
        }
    }

    class ListTools extends TopLevelCommand {

        @Override
        public void run() {
            System.out.println("Tools supported by this build of bakemono:");
            System.out.println();
            for(Map.Entry<String,Tool> i:context.getBeansOfType(Tool.class).entrySet()) {
                System.out.println("    "+i.getKey());
            }
        }

    }
}
