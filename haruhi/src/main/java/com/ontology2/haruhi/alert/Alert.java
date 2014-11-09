package com.ontology2.haruhi.alert;

import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.google.common.base.Joiner.on;

@Component("alert")
public class Alert extends CommandLineApplication {
    @Autowired
    AlertService alertService;
    @Override
    protected void _run(String[] arguments) throws Exception {
        alertService.alert(on(" ").join(arguments));
    }
}
