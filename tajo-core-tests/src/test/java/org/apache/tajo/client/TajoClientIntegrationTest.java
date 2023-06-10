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

import org.apache.tajo.exception.TajoException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.apache.tajo.QueryTestCaseBase.getConf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TajoClientIntegrationTest {
    private TajoClientImplForTesting tajoClient;

    private CatalogAdminClientImpl catalogAdminClient = mock(CatalogAdminClientImpl.class);
    private QueryClientImpl queryClient = mock(QueryClientImpl.class);

    @BeforeEach
    public void setUp() throws SQLException {
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(getConf());

        this.tajoClient = new TajoClientImplForTesting(serviceTracker,"default",this.queryClient,this.catalogAdminClient);
    }
    @AfterEach
    public void tearDown(){
        if(this.tajoClient != null)
            this.tajoClient.close();
    }

    @Test
    public void tajoClientIT() throws TajoException {
        String dbName = "CreatedDB";
        String createTable = "CREATE TABLE TestingTable (a1 int, a2 char);";

        //CreateDB and verify that it catalog is correctly called
        doNothing().when(this.catalogAdminClient).createDatabase(anyString());
        this.tajoClient.createDatabase(dbName);
        verify(this.catalogAdminClient, times(1)).createDatabase(dbName);

        //Now check if is correctly called execute query
        when(this.queryClient.executeQuery(anyString())).thenAnswer(invocation-> {
            System.out.println("Executed query");
            return null;
        });
        this.tajoClient.executeQuery(createTable);
        verify(this.queryClient, times(1)).executeQuery(createTable);


        //Now check if is correctly called executeQueryAndGetResult
        when(this.queryClient.executeQueryAndGetResult(anyString())).thenAnswer(invocation-> {
            System.out.println("Executed query, now return results");
            return null;
        });
        this.tajoClient.executeQueryAndGetResult(createTable);
        verify(this.queryClient, times(1)).executeQueryAndGetResult(createTable);

        //Check if is correctly called one time existDB
        when(this.catalogAdminClient.existDatabase(anyString())).thenAnswer(invocation-> {
            System.out.println("The database exists");
            return true;
        });;
        boolean exists = this.tajoClient.existDatabase(dbName);
        verify(this.catalogAdminClient, times(1)).existDatabase(dbName);
        Assertions.assertTrue(exists);

        //Check if is correctly called one time dropDB
        doNothing().when(this.catalogAdminClient).dropDatabase(anyString());
        this.tajoClient.dropDatabase(dbName);
        verify(this.catalogAdminClient, times(1)).dropDatabase(dbName);
    }
}
