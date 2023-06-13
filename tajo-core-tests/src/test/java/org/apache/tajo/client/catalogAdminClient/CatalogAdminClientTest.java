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
import org.apache.tajo.exception.*;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.tajo.client.QueryTestCaseBase.getConf;

public class CatalogAdminClientTest {
    //Pattern for db name, now we accept also - _ as special character
    private final String regexPattern = "[^a-zA-Z0-9_-]";
    private final Pattern pattern = Pattern.compile(this.regexPattern);
    private Matcher matcher;
    private final CatalogAdminClient mockedCatalogAdminClient;

    private CatalogAdminClientImpl catalogAdminClient;

    private List<String> databaseList;
    private List<String> privilegeDatabaseList;

    static Stream<CatalogParameterSet> createDatabaseParameters(){

        //Unidimensional selection for createDatabase boundary values
        return Stream.of(
                //valid db name
                new CatalogParameterSet("db1","db2",false, null),
                //case sensitive
                new CatalogParameterSet("db1","DB1",false, null),
                //not valid db name (already exists)
                new CatalogParameterSet("db1","db1",true, DuplicateDatabaseException.class.getName())
                //empty db name
                //new CatalogParameterSet("db1","",true, Exception.class.getName()),
                //null db name
                //new CatalogParameterSet("db1",null,true, Exception.class.getName()),
                // special char
                // new CatalogParameterSet("$Sp3_c|al!",null,false,true, Exception.class.getName())

        );
    }

    static Stream<CatalogParameterSet> existsDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new CatalogParameterSet("db1","db1",true,false, null),
                //not valid db name (db doesn't exists, case sensitive)
                new CatalogParameterSet("db1","DB1",false,false, null),
                //not valid db name (db doesn't exists)
                new CatalogParameterSet("db1","db3",false,false, null)

                );
    }

    static Stream<CatalogParameterSet> dropDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new CatalogParameterSet("db1","db2","db1",false, null),
                //not valid db name (db doesn't exists, case sensitive)
                new CatalogParameterSet("db1","db2","DB1",true, UndefinedDatabaseException.class.getName()),
                //not valid db name (db doesn't exists)
                new CatalogParameterSet("db1","db2","db3",true, UndefinedDatabaseException.class.getName()),
                //no permission
                new CatalogParameterSet("db1","db2","default",true, CannotDropCurrentDatabaseException.class.getName()),
                //no permission
                 new CatalogParameterSet("db1","db2","information_schema",true, InsufficientPrivilegeException.class.getName())

                //empty db name
               // new CatalogParameterSet("db1","db2","",true, Exception.class.getName()),
                //null db name
               // new CatalogParameterSet("db1","db2",null,true, Exception.class.getName())
        );
    }



    public CatalogAdminClientTest(){
        // Mocked catalog admin client
        this.mockedCatalogAdminClient = Mockito.mock(CatalogAdminClient.class);
        this.databaseList = new ArrayList<>();
        this.databaseList.add("default");
        this.databaseList.add("information_schema");
        this.privilegeDatabaseList = new ArrayList<>();
        this.databaseList.add("information_schema");
    }

    /** Define with mock a dummy implementation of create, exists and drop DB. <br />
     * After that, test the real implementation and check if the result are the same.*/
    @BeforeEach
    public void configureMocks() throws Exception {
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(getConf());
        this.catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));

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

    @ParameterizedTest
    @MethodSource("createDatabaseParameters")
    public void createDatabaseTest(CatalogParameterSet catalogParameterSet){
        boolean isExceptionThrownOne;
        boolean isExceptionThrownTwo;
        Exception e1 = null, e2 = null;
        //CreateDatabase on mocked catalogAdminClient
        try {
            this.mockedCatalogAdminClient.createDatabase(catalogParameterSet.getExistingDB());
            this.mockedCatalogAdminClient.createDatabase(catalogParameterSet.getCreateDB());
            //if expectedException was false, test is gone correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(catalogParameterSet.isExpectedException());
        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            Assertions.assertNotNull(catalogParameterSet.getExceptionClass());
            Assertions.assertTrue(catalogParameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!catalogParameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(catalogParameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //CreateDatabase on catalogAdminClient implementation
        try {
            this.catalogAdminClient.createDatabase(catalogParameterSet.getExistingDB());
            this.catalogAdminClient.createDatabase(catalogParameterSet.getCreateDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(catalogParameterSet.isExpectedException());
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(catalogParameterSet.getExceptionClass());
            Assertions.assertTrue(catalogParameterSet.isExpectedException());
            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!catalogParameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(catalogParameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2;
            }
        }

        Assertions.assertEquals(isExceptionThrownOne,isExceptionThrownTwo,"Both the execution have thrown exception");
        if (e1 != null && e2 != null)
            Assertions.assertEquals(e1.getClass().getName(),e2.getClass().getName(),"Both the executions have thrown the same exception");
        System.out.println("Both the executions have been the same result");
    }

    @ParameterizedTest
    @MethodSource("existsDatabaseParameters")
    public void existsDatabaseTest(CatalogParameterSet catalogParameterSet){
        boolean isExceptionThrownOne,isExceptionThrownTwo;
        boolean existsOne = false, existsTwo = false;

        Exception e1 = null, e2 = null;
        //CreateDatabase on mocked catalogAdminClient and check if exists
        try {
            this.mockedCatalogAdminClient.createDatabase(catalogParameterSet.getExistingDB());
            existsOne = this.mockedCatalogAdminClient.existDatabase(catalogParameterSet.getExistDB());
            //if expectedException is false, test is going correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(catalogParameterSet.isExpectedException());
            //if expected exist result and the result are equals, test is going correctly
            Assertions.assertEquals(existsOne, catalogParameterSet.isExistResult());

        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            //Check if exception is expected
            Assertions.assertNotNull(catalogParameterSet.getExceptionClass());
            Assertions.assertTrue(catalogParameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!catalogParameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(catalogParameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //CreateDatabase on catalogAdminClient implementation
        try {
            this.catalogAdminClient.createDatabase(catalogParameterSet.getExistingDB());
            existsTwo = this.catalogAdminClient.existDatabase(catalogParameterSet.getExistDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(catalogParameterSet.isExpectedException());
            //if expected exist result and the result are equals, test is going correctly
            Assertions.assertEquals(existsTwo, catalogParameterSet.isExistResult());
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            //Check if exception is expected
            Assertions.assertNotNull(catalogParameterSet.getExceptionClass());
            Assertions.assertTrue(catalogParameterSet.isExpectedException());

            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!catalogParameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(catalogParameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2;
            }
        }

        Assertions.assertEquals(isExceptionThrownOne,isExceptionThrownTwo,"Not both the execution have thrown exception");
        if (e1 != null && e2 != null)
            Assertions.assertEquals(e1.getClass().getName(),e2.getClass().getName(),"Not both the executions have thrown the same exception");

        Assertions.assertEquals(existsOne,existsTwo,"Not both the executions have the same output");
        System.out.println("Both the executions have been the same result");
    }

    /* Those tests don't fail cause thrown UndefinedNameException not for error caused by the input */
    @ParameterizedTest
    @MethodSource("dropDatabaseParameters")
    public void dropDatabaseTest(CatalogParameterSet catalogParameterSet){
        boolean isExceptionThrownTwo;
        String  e2 = null;
        //DropDatabase on catalogAdminClient implementation
        try {
            this.catalogAdminClient.createDatabase(catalogParameterSet.getExistingDB());
            this.catalogAdminClient.createDatabase(catalogParameterSet.getCreateDB());
            this.catalogAdminClient.dropDatabase(catalogParameterSet.getDropDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(catalogParameterSet.isExpectedException());
        }
        catch (UndefinedDatabaseException | CannotDropCurrentDatabaseException | InsufficientPrivilegeException actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(catalogParameterSet.getExceptionClass());
            Assertions.assertTrue(catalogParameterSet.isExpectedException());
            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!catalogParameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(catalogParameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2.getClass().getName();
            }
        }
        catch(TajoInternalError actualError){
            isExceptionThrownTwo = true;
            if( actualError.getMessage().contains("DROP_CURRENT_DATABASE"))
             e2 = CannotDropCurrentDatabaseException.class.getName();
            else e2 = "Something different";
        }
        catch (DuplicateDatabaseException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(catalogParameterSet.isExpectedException(),isExceptionThrownTwo,"Doesn't  thrown exception");
        if ( e2 != null)
            Assertions.assertEquals(catalogParameterSet.getExceptionClass(),e2,"Not the same exception");
        System.out.println("Test has done the predicted result");
    }

    @AfterEach
    void tearDown() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
        //Remove remaining database except the default ones
        List<String> databases = new ArrayList<>(this.catalogAdminClient.getAllDatabaseNames());

        databases.remove("information_schema");
        databases.remove("default");

        for(String database: databases)
            this.catalogAdminClient.dropDatabase(database);

        if(this.catalogAdminClient!= null)
            this.catalogAdminClient.close();
    }


}
