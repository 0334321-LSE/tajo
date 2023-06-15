package org.apache.tajo.client.queryClient;
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
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.client.QueryClientImpl;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.*;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.tajo.Assert.assertNotNull;
import static org.apache.tajo.client.QueryTestCaseBase.getConf;

public class QueryClientTest  {

    private QueryClientImpl queryClient;


    @BeforeEach
    public void createDB() throws Exception {
        String createTable = ("CREATE TABLE TestingTable (a1 int, a2 char);");
        String insertInto ="INSERT INTO TestingTable values (7, 'T');";
        ServiceTracker serviceTracker = ServiceTrackerFactory.get(getConf());
        this.queryClient = new QueryClientImpl(new SessionConnection(serviceTracker,"default",new KeyValueSet()));
        this.queryClient.executeQuery(createTable);
        createTable = ("CREATE TABLE anotherTable (a1 int, a2 char);");
        this.queryClient.executeQuery(createTable);
        this.queryClient.executeQuery(insertInto);

    }

    static Stream<QueryParameterSet> executeQueryParameters(){
        return Stream.of(
                //Query valida
                new QueryParameterSet("SELECT * FROM testingTable;",false, Errors.ResultCode.OK),
                //Query non valida (sintassi)
                new QueryParameterSet("SELECT ! FROM testingTable",false, Errors.ResultCode.SYNTAX_ERROR),
                //Query non valida (parametri)
                new QueryParameterSet("INSERT INTO testingTable values (String, 3)",false, Errors.ResultCode.UNDEFINED_COLUMN),
                //Query vuota
                new QueryParameterSet("",false, Errors.ResultCode.INTERNAL_ERROR),
                //Query null
                new QueryParameterSet(null,true,null)
        );
    }
    static Stream<QueryParameterSet> executeQueryAndGetResultParameters(){
        return Stream.of(
                //Query valida
                new QueryParameterSet("SELECT * FROM testingTable;",false),
                //Query non valida (sintassi)
                new QueryParameterSet("SELECT ! FROM testingTable",true),
                //Query non valida (parametri)
                new QueryParameterSet("INSERT INTO testingTable values ('A', 3);",true),
                //Query vuota
                new QueryParameterSet("",true),
                //Query null
                new QueryParameterSet(null,true)

                // Query for jacoco to reach fetch, block the execution
                // new QueryParameterSet("SELECT * FROM testingTable WHERE a1 < 3",false)

                // Query for jacoco
                // new QueryParameterSet("SELECT a1 FROM anotherTable ORDER BY a2",false)

                );
    }
    static Stream<QueryParameterSet>getQueryStatusResultParameters(){
        return Stream.of(
                //Query valida
                new QueryParameterSet("VALIDA",false, TajoProtos.QueryState.QUERY_SUCCEEDED.toString(), null),
                //QueryID non valida
                new QueryParameterSet("NON_VALIDA",true, null, null),
                //QueryID null
                new QueryParameterSet("NULLA",true, null, null),
              // combinazioni sulla forma di queryID
                //String valid, int positive
                new QueryParameterSet("VP",false, TajoProtos.QueryState.QUERY_SUCCEEDED.toString(), null),
                //String valid, int negative
                new QueryParameterSet("VN",true, null, null),
                //String not valid, int positive
                new QueryParameterSet("NP",true, null, null),
                //String not valid, int negative
                new QueryParameterSet("NN",true, null, null),
                //String void, int positive
                new QueryParameterSet("VoP",true, null, null),
                //String void, int negative
                new QueryParameterSet("VoN",true, null, null),
                //String null, int positive
                new QueryParameterSet("NuP",true, null, null),
                //String null, int negative
                new QueryParameterSet("NuN",true, null, null)


        );
    }

    @ParameterizedTest @Disabled
    @MethodSource("executeQueryParameters")
    public void executeQueryTest(QueryParameterSet queryParameterSet){
        try{
            //Check if the expected returnCode is equals to the real one
            ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery(queryParameterSet.getQuery());
            Assertions.assertEquals(queryParameterSet.getError(),response.getState().getReturnCode());
            System.out.println("Error code" + response.getState().getReturnCode());
        }catch (Exception e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        }
    }

    @ParameterizedTest @Disabled
    @MethodSource("executeQueryAndGetResultParameters")
    public void executeAndGetResultTest(QueryParameterSet queryParameterSet){
        try{
            ResultSet resultSet = this.queryClient.executeQueryAndGetResult(queryParameterSet.getQuery());
            if(queryParameterSet.isExpectedException())
                Assertions.fail();

            resultSet.next();
            int resultInteger = resultSet.getInt(1);
            String resultCharacter = resultSet.getString(2);
            boolean control1 = resultInteger == 7;
            boolean control2 = resultCharacter.equals("T");
            Assertions.assertEquals(control1,control2);

        }catch(RuntimeException | TajoInternalError | TajoException | SQLException e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        }
    }

    @ParameterizedTest @Disabled
    @MethodSource("getQueryStatusResultParameters")
    public void getQueryStatusTest(QueryParameterSet queryParameterSet){
        String statusName;
        switch (queryParameterSet.getQuery()){
            case "VALIDA":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId())).getState().name();
                    Assertions.assertEquals(statusName,queryParameterSet.getQueryStatus());
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
                    Assertions.assertTrue(true);
                    break;
                }
            case "NULLA":
                try {
                    statusName = this.queryClient.getQueryStatus(null).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }
            //Casi di test
            case "VP":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId().getId(),1)).getState().name();
                    Assertions.assertEquals(statusName,queryParameterSet.getQueryStatus());
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }
            case "VN":
                try {
                    ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery("SELECT * FROM testingTable;");
                    statusName = this.queryClient.getQueryStatus(new QueryId(response.getQueryId().getId(),-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }
            case "NP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("something",1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }

            case "NN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("something",-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }
            case "VoN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("",-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }

            case "VoP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId("",1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }

            case "NuN":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId(null,-1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }

            case "NuP":
                try {
                    statusName = this.queryClient.getQueryStatus(new QueryId(null,1)).getState().name();
                    Assertions.fail();
                    break;
                } catch (QueryNotFoundException | NullPointerException exception) {
                    Assertions.assertTrue(true);
                    break;
                }
        }


    }


    @AfterEach
    public void tearDownDB() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

        if(this.queryClient != null)
            this.queryClient.close();

    }


}
