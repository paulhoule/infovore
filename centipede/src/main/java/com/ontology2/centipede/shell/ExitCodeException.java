package com.ontology2.centipede.shell;

import java.util.IllegalFormatException;

/**
 * This represents the exit of a POSIX or DOS process with a non-zero
 * exit status.  Although the status is represented as a 32-bit integer,
 * 
 * It is fair to throw this exception if another process has failed with
 * an exception,  to represent the failure of that other process.
 * 
 * It is also fair to throw this exception if you want the process you
 * are running in now shut down with the given exit code,  at least
 * if you are running inside a CommandLineApplication.
 * 
 * This means that a failing external process causes the current process
 * to abort, returning the same error code as the failing external process
 * unless you catch the ExitCodeException somewhere.
 * 
 * Converting java exceptions in general to ExitCodes in general,  such as
 * IOException() which would reasonably map to EX_IOERR
 */

public class ExitCodeException extends Exception {
    final int status;
    
    //
    // want to leave the option of converting to a subclass if desired,  to
    // implement finer-grained exception types such as one would see in a
    // POSIX sysexits.h
    //
    // http://www.opensource.apple.com/source/Libc/Libc-320/include/sysexits.h
    //
    
    public static ExitCodeException create(int status) {
        return new ExitCodeException(status);
    }

    protected ExitCodeException(int status) {
        super("process exit code "+status);
        
        if (status<1 || status>255) {
            throw new IllegalArgumentException();
        }
        
        this.status=status;
    }
    
    public int getStatus() {
        return status;
    }
    

    public static int EX_USAGE=64;  /* command line usage error */
    public static int EX_DATAERR=65;  /* data format error */
    public static int EX_NOINPUT=66;  /* cannot open input */
    public static int EX_NOUSER=67;  /* addressee unknown */
    public static int EX_NOHOST=68;  /* host name unknown */
                    
    public static int EX_UNAVAILABLE=69;  /* service unavailable */
    public static int EX_SOFTWARE=70;  /* internal software error */
    public static int EX_OSERR=71;  /* system error (e.g., can't fork) */
    public static int EX_OSFILE=72;  /* critical OS file missing */
    public static int EX_CANTCREAT=73;  /* can't create (user) output file */
    public static int EX_IOERR=74;  /* input/output error */
    public static int EX_TEMPFAIL=75;  /* temp failure; user is invited to retry */
    public static int EX_PROTOCOL=76;  /* remote error in protocol */
    public static int EX_NOPERM=77;  /* permission denied */
    public static int EX_CONFIG=78;  /* configuration error */
    
}
