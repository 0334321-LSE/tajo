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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.junit.Ignore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CatalogAdminClientTest {
    //Pattern for db name, now we accept also - _ as special character
    private final String regexPattern = "[^a-zA-Z0-9]";
    private final Pattern pattern = Pattern.compile(this.regexPattern);
    private Matcher matcher;
    private final CatalogAdminClient mockedCatalogAdminClient;

    private ArrayList<String> databaseList;

    private static TajoTestingCluster cluster;
    private TajoClient client;

    static Stream<ParameterSet> createDatabaseParameters(){

        //Unidimensional selection for createDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2",false, null),
                //case sensitive
                new ParameterSet("db1","DB2",false, null),
                //not valid db name (already exists)
                new ParameterSet("db1","db1",true, DuplicateDatabaseException.class.getName()),
                //empty db name
                new ParameterSet("db1","",true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1",null,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","*$p&cialDB!*",true, Exception.class.getName())
        );
    }

    static Stream<ParameterSet> existsDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db1",true,false, null),
                //not valid db name (db doesn't exists, case sensitive)
                new ParameterSet("db1","DB1",false,false, null),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db3",false,false, null),
                //empty db name
                new ParameterSet("db1","",false,true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1",null,false,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","*$p&cialDB!*",false,true, Exception.class.getName())
                );
    }

    static Stream<ParameterSet> dropDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2","db1",false, null),
                //not valid db name (db doesn't exists, case sensitive)
                new ParameterSet("db1","DB1","db3",true, UndefinedDatabaseException.class.getName()),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db2","db3",true, UndefinedDatabaseException.class.getName()),
                //empty db name
                new ParameterSet("db1","db2","",true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1","db2",null,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","db2","*$p&cialDB!*",true, Exception.class.getName())
        );
    }



    public CatalogAdminClientTest(){
        // Mocked catalog admin client
        this.mockedCatalogAdminClient = Mockito.mock(CatalogAdminClient.class);
        this.databaseList = new ArrayList<>();
    }

    @BeforeAll
    public static void setUp() {
        cluster = TpchTestBase.getInstance().getTestingCluster();
    }

    /** Define with mock a dummy implementation of create, exists and drop DB. <br />
     * After that, test the real implementation and check if the result are the same.*/
    @BeforeEach
    public void configureMocks() throws Exception {
        this.client = cluster.newTajoClient();
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
            else
                //Creation simply is represented by adding the db to an array list
                this.databaseList.remove(dbName);

            return null;
        }).when(mockedCatalogAdminClient).dropDatabase(Matchers.anyString());
    }

    @ParameterizedTest @Disabled
    @MethodSource("createDatabaseParameters")
    public void createDatabaseTest(ParameterSet parameterSet){
        boolean isExceptionThrownOne;
        boolean isExceptionThrownTwo;
        Exception e1 = null, e2 = null;
        //CreateDatabase on mocked catalogAdminClient
        try {
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getExistingDB());
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getCreateDB());
            //if expectedException was false, test is gone correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(parameterSet.isExpectedException());
        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(parameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //CreateDatabase on catalogAdminClient implementation
        try {
            client.createDatabase(parameterSet.getExistingDB());
            client.createDatabase(parameterSet.getCreateDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(parameterSet.isExpectedException());
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());
            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(parameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2;
            }
        }

        Assertions.assertEquals(isExceptionThrownOne,isExceptionThrownTwo,"Both the execution have thrown exception");
        if (e1 != null && e2 != null)
            Assertions.assertEquals(e1.getClass().getName(),e2.getClass().getName(),"Both the executions have thrown the same exception");
        System.out.println("Both the executions have been the same result");
    }

    @ParameterizedTest @Disabled
    @MethodSource("existsDatabaseParameters")
    public void existsDatabaseTest(ParameterSet parameterSet){
        boolean isExceptionThrownOne,isExceptionThrownTwo;
        boolean existsOne = false, existsTwo = false;

        Exception e1 = null, e2 = null;
        //CreateDatabase on mocked catalogAdminClient and check if exists
        try {
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getExistingDB());
            existsOne = this.mockedCatalogAdminClient.existDatabase(parameterSet.getExistDB());
            //if expectedException is false, test is going correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(parameterSet.isExpectedException());
            //if expected exist result and the result are equals, test is going correctly
            Assertions.assertEquals(existsOne,parameterSet.isExistResult());

        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            //Check if exception is expected
            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(parameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //CreateDatabase on catalogAdminClient implementation
        try {
            client.createDatabase(parameterSet.getExistingDB());
            existsTwo = client.existDatabase(parameterSet.getExistDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(parameterSet.isExpectedException());
            //if expected exist result and the result are equals, test is going correctly
            Assertions.assertEquals(existsTwo,parameterSet.isExistResult());
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            //Check if exception is expected
            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());

            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(parameterSet.getExceptionClass(), actualE2.getClass().getName());
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
    public void dropDatabaseTest(ParameterSet parameterSet){
        boolean isExceptionThrownOne;
        boolean isExceptionThrownTwo;
        Exception e1 = null, e2 = null;
        //DropDatabase on mocked catalogAdminClient
        try {
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getExistingDB());
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getCreateDB());
            this.mockedCatalogAdminClient.dropDatabase(parameterSet.getDropDB());
            //if expectedException was false, test is gone correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(parameterSet.isExpectedException());
        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(parameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //DropDatabase on catalogAdminClient implementation
        try {
            client.createDatabase(parameterSet.getExistingDB());
            client.createDatabase(parameterSet.getCreateDB());
            client.dropDatabase(parameterSet.getDropDB());
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(parameterSet.isExpectedException());
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());
            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(parameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2;
            }
        }

        Assertions.assertEquals(isExceptionThrownOne,isExceptionThrownTwo,"Both the execution have thrown exception");
        if (e1 != null && e2 != null)
            Assertions.assertEquals(e1.getClass().getName(),e2.getClass().getName(),"Both the executions have thrown the same exception");
        System.out.println("Both the executions have been the same result");
    }

    @AfterEach
    void tearDown() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
        //Remove remaining database except the default ones
        List<String> databases = new ArrayList<>(client.getAllDatabaseNames());

        databases.remove("information_schema");
        databases.remove("default");

        for(String database: databases)
            client.dropDatabase(database);

        if(client!= null)
            client.close();
    }


}
