package com.ontology2.centipede.shell;

public class ExternalProcessFailedWithErrorCode extends Exception {
    private int code;

    public int getCode() {
        return code;
    }

    public ExternalProcessFailedWithErrorCode(int code) {
        super("External process failed with code "+code);
        this.code=code;
    };
}
