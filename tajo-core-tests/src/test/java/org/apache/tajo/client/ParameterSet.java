package org.apache.tajo.client;

public class ParameterSet {
    private  String existingDB;
    private  String createDB;
    private  String existDB;
    private  String dropDB;
    private  boolean existResult;
    private final boolean expectedException;

    private String exceptionClass;

    // Parameters for createDatabase
    public ParameterSet(String existingDB, String createDB, boolean expectedException, String exceptionClass) {
        this.existingDB = existingDB;
        this.createDB = createDB;
        this.expectedException = expectedException;
        this.exceptionClass = exceptionClass;
    }

    // Parameters for existsDatabase
    public ParameterSet(String existingDB, String existDB, boolean existResult, boolean expectedException, String exceptionClass) {
        this.existingDB = existingDB;
        this.existDB = existDB;
        this.existResult = existResult;
        this.expectedException = expectedException;
        this.exceptionClass = exceptionClass;
    }

    // Parameter for dropDatabase
    public ParameterSet(String existingDB, String createDB,String dropDB, boolean expectedException, String exceptionClass) {
        this.existingDB = existingDB;
        this.createDB = createDB;
        this.dropDB = dropDB;
        this.expectedException = expectedException;
        this.exceptionClass = exceptionClass;
    }

    public String getExistingDB() {
        return existingDB;
    }

    public String getCreateDB() {
        return createDB;
    }

    public String getExceptionClass() {
        return exceptionClass;
    }

    public String getExistDB() {
        return existDB;
    }

    public String getDropDB() {
        return dropDB;
    }

    public boolean isExistResult() {
        return existResult;
    }

    public boolean isExpectedException() {
        return expectedException;
    }
}

