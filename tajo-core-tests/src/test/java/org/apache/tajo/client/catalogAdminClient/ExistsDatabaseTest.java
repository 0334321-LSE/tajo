package org.apache.tajo.client.catalogAdminClient;

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

import org.apache.tajo.client.CatalogAdminClient;
import org.apache.tajo.client.CatalogAdminClientImpl;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.tajo.client.QueryTestCaseBase.getConf;

@RunWith(Parameterized.class)
public class ExistsDatabaseTest {

    private final String regexPattern = "[^a-zA-Z0-9_-]";
    private final Pattern pattern = Pattern.compile(this.regexPattern);
    private Matcher matcher;    private CatalogAdminClient mockedCatalogAdminClient;

    // Test parameters
    private CatalogAdminClientImpl catalogAdminClient;

    private List<String> databaseList;

    private List<String> privilegeDatabaseList;

    private  String existingDB;
    private  String existDB;

    private final boolean existResult;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                //valid db name
                {"db1","db1",true},
                //not valid db name (db doesn't exists, case sensitive)
                {"db1","DB1",false},
                //not valid db name (db doesn't exists)
                {"db1","db3",false}

        });
    }

    public ExistsDatabaseTest(String existingDB, String existDB, boolean existResult) throws DuplicateDatabaseException {
        this.existingDB = existingDB;
        this.existDB = existDB;
        this.existResult = existResult;

    }

    @Before
    public void setUp(){
        // Mocked catalog admin client
        this.mockedCatalogAdminClient = Mockito.mock(CatalogAdminClient.class);
        this.databaseList = new ArrayList<>();
        this.databaseList.add("default");
        this.databaseList.add("information_schema");
        this.privilegeDatabaseList = new ArrayList<>();
        this.privilegeDatabaseList.add("information_schema");

        ServiceTracker serviceTracker = ServiceTrackerFactory.get(getConf());
        this.catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));

    }

    @After
    public void tearDown() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
        //Remove remaining database except the default ones
        List<String> databases = new ArrayList<>(this.catalogAdminClient.getAllDatabaseNames());

        databases.remove("information_schema");
        databases.remove("default");

        for(String database: databases)
            this.catalogAdminClient.dropDatabase(database);

        if(this.catalogAdminClient!= null)
            this.catalogAdminClient.close();
    }

    @Test
    public void existsDatabaseTest() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
        boolean existsOne, existsTwo;
        this.configureMocks();
        //CreateDatabase on mocked catalogAdminClient and check if exists

        this.mockedCatalogAdminClient.createDatabase(this.existingDB);
        existsOne = this.mockedCatalogAdminClient.existDatabase(this.existDB);
        //if expectedException is false, test is going correctly

        //if expected exist result and the result are equals, test is going correctly
        Assertions.assertEquals(existsOne, this.existResult);



        //CreateDatabase on catalogAdminClient implementation
        this.catalogAdminClient.createDatabase(this.existingDB);
        existsTwo = this.catalogAdminClient.existDatabase(this.existDB);


        //if expected exist result and the result are equals, test is going correctly
        Assertions.assertEquals(existsTwo, this.existResult);


        Assertions.assertEquals(existsOne,existsTwo,"Not both the executions have the same output");
        System.out.println("Both the executions have been the same result");
    }

    public void configureMocks() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
        // Mocked CreateDatabase, it does 3 check before the creation
        Mockito.doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName contains special characters ( expected _ or -)
            this.matcher = this.pattern.matcher(dbName);
            if (this.matcher.find())
                throw new IllegalArgumentException("DB name contains special characters .");

            //3) Check if dbName already exists
            if (this.databaseList.contains(dbName))
                throw new DuplicateDatabaseException("DB: "+dbName+ " already exists");
            else
                //Creation simply is represented by adding the db to an array list
                this.databaseList.add(dbName);

            return null;
        }).when(mockedCatalogAdminClient).createDatabase(Matchers.anyString());

        /* Mocked existDatabase, it only checks if there is a database with the same name
         of the input argument */
        Mockito.doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName is in the list
            if(this.databaseList.contains(dbName))
                return true;
            else
                return false;
        }).when(mockedCatalogAdminClient).existDatabase(Matchers.anyString());

        // Mocked dropDatabase, it does 3 check before the creation
        Mockito.doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName contains special characters ( expected _ or -)
            this.matcher = this.pattern.matcher(dbName);
            if (this.matcher.find())
                throw new IllegalArgumentException("DB name contains special characters .");

            //3) Check if dbName not exists in the list
            if (!this.databaseList.contains(dbName))
                throw new UndefinedDatabaseException("DB name doesn't exist");

            //4) Check if dbName isn't in default db names
            if (this.privilegeDatabaseList.contains(dbName))
                throw  new InsufficientPrivilegeException("You don't have permission to drop this db");

            //5) Db is currently in use
            if (dbName.equals("default"))
                throw  new CannotDropCurrentDatabaseException();
            else
                //Remove from list
                this.databaseList.remove(dbName);

            return null;
        }).when(mockedCatalogAdminClient).dropDatabase(Matchers.anyString());
    }

}
