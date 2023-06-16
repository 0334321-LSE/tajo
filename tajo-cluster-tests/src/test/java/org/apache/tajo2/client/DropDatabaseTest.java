package org.apache.tajo2.client;

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

import org.apache.tajo.exception.*;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo2.util.QueryTestCaseBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class DropDatabaseTest {

    private org.apache.tajo2.client.CatalogAdminClientImpl catalogAdminClient;
    private String existingDB;
    private String dropDB;
    private boolean expectedException;
    private String exceptionClass;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                //valid db name
                {"db1","db1",false, null},
                //not valid db name (db doesn't exists, case sensitive)
                {"db1","DB1",true, UndefinedDatabaseException.class.getName()},
                //not valid db name (db doesn't exists)
                {"db1","db3",true, UndefinedDatabaseException.class.getName()},
                //no permission, current DB
                {"db1","default",true, CannotDropCurrentDatabaseException.class.getName()},
                //no permission, no privilege
                {"db1","information_schema",true, InsufficientPrivilegeException.class.getName()}

                //empty db name
                // {"db1","",true, Exception.class.getName()},
                //null db name
                // {"db1",null,true, Exception.class.getName()}
        });
    }

    public DropDatabaseTest(String existingDB, String dropDB, boolean expectedException, String exceptionClass){
        this.existingDB = existingDB;
        this.dropDB = dropDB;
        this.expectedException = expectedException;
        this.exceptionClass = exceptionClass;
    }
    @Before
    public void setUp(){
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(QueryTestCaseBase.getConf());
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

    /* Last two tests don't fail cause thrown UndefinedDatabaseException not for error caused by the input */
    @Test
    public void dropDatabaseTest(){
        boolean isExceptionThrownTwo;
        String  e2 = null;
        //DropDatabase on catalogAdminClient implementation
        try {
            this.catalogAdminClient.createDatabase(this.existingDB);
            this.catalogAdminClient.dropDatabase(this.dropDB);
            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(this.expectedException);
        }
        catch (UndefinedDatabaseException | CannotDropCurrentDatabaseException | InsufficientPrivilegeException actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(this.exceptionClass);
            Assertions.assertTrue(this.expectedException);
            System.out.println("Thrown exception :"+actualE2.getClass().getName());
            // If it isn't unknown type of exception, check if it is the right one
            if(!this.exceptionClass.equals(Exception.class.getName())) {
                Assertions.assertEquals(this.exceptionClass, actualE2.getClass().getName());
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
        Assertions.assertEquals(this.expectedException,isExceptionThrownTwo,"Exception problem");
        if ( e2 != null)
            Assertions.assertEquals(this.exceptionClass,e2,"Not the same exception");
        System.out.println("Test has done the predicted result");
    }


}
