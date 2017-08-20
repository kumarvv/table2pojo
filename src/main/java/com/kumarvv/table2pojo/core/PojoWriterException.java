package com.kumarvv.table2pojo.core;

public class PojoWriterException extends Exception {
    public PojoWriterException() {
        super();
    }

    public PojoWriterException(String message) {
        super(message);
    }

    public PojoWriterException(String message, Throwable cause) {
        super(message, cause);
    }

    public PojoWriterException(Throwable cause) {
        super(cause);
    }

    protected PojoWriterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
