package org.apache.tajo.client;
/*
        Licensed to the Apache Software Foundation (ASF) under one
        or more contributor license agreements.  See the NOTICE file
        distributed with this work for additional information
        regarding copyright ownership.  The ASF licenses this file
        to you under the Apache License, Version 2.0 (the
        "License"); you may not use this file except in compliance
        with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.
        */
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

