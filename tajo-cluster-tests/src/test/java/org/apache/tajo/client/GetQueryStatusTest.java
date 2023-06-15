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

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.client.QueryClientImpl;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.exception.QueryNotFoundException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.tajo.util.QueryTestCaseBase.getConf;
@RunWith(Parameterized.class)
public class GetQueryStatusTest {
    private QueryClientImpl queryClient;
    private  String query;
    private  boolean expectedException;

    private String queryStatus;

    public String getQuery() {
        return query;
    }




    public GetQueryStatusTest(String query, boolean expectedException, String queryStatus ) {
        this.query = query;
        this.expectedException = expectedException;
        this.queryStatus = queryStatus;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                //Query valida
                {"VALIDA",false, TajoProtos.QueryState.QUERY_SUCCEEDED.toString()},
                //QueryID non valida
                {"NON_VALIDA",true, null},
                //QueryID null
                {"NULLA",true, null},

                // combinazioni sulla forma di queryID
                //String valid, int positive
                 {"VP",false, TajoProtos.QueryState.QUERY_SUCCEEDED.toString()},
                //String valid, int negative
                {"VN",true, null},
                //String not valid, int positive
                {"NP",true, null},
                //String not valid, int negative
                {"NN",true, null},
                //String void, int positive
                {"VoP",true, null},
                //String void, int negative
                {"VoN",true, null},
                //String null, int positive
                {"NuP",true, null},
                //String null, int negative
                {"NuN",true, null}

        });
    }
    @Before
    public void createDB()  {
        String createTable = ("CREATE TABLE TestingTable (a1 int, a2 char);");
        String insertInto ="INSERT INTO TestingTable values (7, 'T');";
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(getConf());
        this.queryClient = new QueryClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));
        this.queryClient.executeQuery(createTable);
        createTable = ("CREATE TABLE anotherTable (a1 int, a2 char);");
        this.queryClient.executeQuery(createTable);
        this.queryClient.executeQuery(insertInto);

    }
    @Test
    public void getQueryStatusTest(){
        String statusName;
        switch (this.query){
            case "VALIDA":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId())).getState().name();
                    Assertions.assertEquals(statusName,this.queryStatus);
                    break;
                } catch (QueryNotFoundException e) {
                    Assertions.fail();
                    break;
                }
            case "NON_VALIDA":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("NOTVALID",122812)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException e) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
            case "NULLA":
                try {
                    statusName = this.queryClient.getQueryStatus(null).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
                //Casi di test
            case "VP":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId().getId(),1)).getState().name();
                    Assertions.assertEquals(statusName,this.queryStatus);
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
            case "VN":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId().getId(),-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
            case "NP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("something",1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }

            case "NN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("something",-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
            case "VoN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("",-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }

            case "VoP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("",1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }

            case "NuN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId(null,-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }

            case "NuP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId(null,1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(this.expectedException);
                    break;
                }
        }


    }
    @After
    public void tearDownDB()  {

        if(this.queryClient != null)
            this.queryClient.close();

    }

}
