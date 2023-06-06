package org.apache.tajo.client;

import org.apache.tajo.error.Errors;

public class QueryParameterSet {
    private String query;
    private boolean expectedException;

    private Errors.ResultCode error;

    public QueryParameterSet(String query, boolean expectedException, Errors.ResultCode error) {
        this.query = query;
        this.expectedException = expectedException;
        this.error = error;
    }

    public QueryParameterSet(String query, boolean expectedException) {
        this.query = query;
        this.expectedException = expectedException;
    }

    public Errors.ResultCode getError() {
        return error;
    }

    public String getQuery() {
        return query;
    }

    public boolean isExpectedException() {
        return expectedException;
    }
}
