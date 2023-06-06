package org.apache.tajo.client;

public class QueryParameterSet {
    private String query;
    private boolean expectedException;

    public QueryParameterSet(String query, boolean expectedException) {
        this.query = query;
        this.expectedException = expectedException;
    }

    public String getQuery() {
        return query;
    }

    public boolean isExpectedException() {
        return expectedException;
    }
}
