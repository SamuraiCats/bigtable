package com.altamiracorp.bigtable.model.exceptions;

/**
 * Runtime exception used to indicate that a BigTable writer implementation
 * encountered an error when writing mutations
 */
public class MutationsWriteException extends RuntimeException {

    public MutationsWriteException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
