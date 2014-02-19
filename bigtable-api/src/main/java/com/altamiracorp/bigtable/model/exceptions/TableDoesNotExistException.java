package com.altamiracorp.bigtable.model.exceptions;

/**
 * Runtime exception used to indicate that a BigTable implementation
 * could not find a table that is expected to exist
 */
public class TableDoesNotExistException extends RuntimeException {

    public TableDoesNotExistException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
