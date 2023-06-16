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
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo2.util.QueryTestCaseBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.sql.SQLException;

public class ExecuteQueryPitTest {
    private org.apache.tajo2.client.QueryClientImpl queryClient;
    private org.apache.tajo2.client.CatalogAdminClientImpl catalogAdminClient;

    private TajoClientImplForTesting tajoClient;


    @Before
    public void setUp() throws SQLException {
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(QueryTestCaseBase.getConf());
        this.queryClient = new QueryClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));
        this.catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));
        tajoClient = new TajoClientImplForTesting(serviceTracker,"default",this.queryClient,this.catalogAdminClient);
    }
    @After
    public void tearDownDB()  {
        if(this.queryClient != null)
            this.queryClient.close();
    }

    @Test
    public void executeQueryPitTest() throws UndefinedDatabaseException, DuplicateDatabaseException {

        // ADDED FOR PIT
        String varname = "CURRENT_DATABASE";
        String variable = "";
        String createTable = ("CREATE TABLE TestingTable (a1 int, a2 char);");

        try {
            variable = this.queryClient.getSessionVariable(varname);
            Assertions.assertEquals(variable,"default");
        }catch (NoSuchSessionVariableException e){
            Assertions.assertEquals(variable,"");
        }
        variable = "";
        this.tajoClient.createDatabase("db2");
        this.queryClient.selectDatabase("db2");
        this.queryClient.executeQuery(createTable);
        try {
            variable = this.queryClient.getSessionVariable(varname);
            Assertions.assertEquals(variable,"db2");
        }catch (NoSuchSessionVariableException e){
            Assertions.assertEquals(variable,"");
        }
    }
}
