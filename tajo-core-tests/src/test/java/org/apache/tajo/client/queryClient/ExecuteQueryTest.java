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


import org.apache.tajo.client.QueryClientImpl;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.error.Errors;
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

import static org.apache.tajo.client.QueryTestCaseBase.getConf;

@RunWith(Parameterized.class)
public class ExecuteQueryTest {

        private QueryClientImpl queryClient;
        private final String query;
        private final boolean expectedException;

        public String getQuery() {
            return query;
        }

        public boolean isExpectedException() {
            return expectedException;
        }

        private final Errors.ResultCode error;


        public ExecuteQueryTest(String query, boolean expectedException, Errors.ResultCode error) {
            this.query = query;
            this.expectedException = expectedException;
            this.error = error;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> getParameters(){
            return Arrays.asList(new Object[][]{ //Query valida
                    //Query valida
                    {"SELECT * FROM testingTable;",false, Errors.ResultCode.OK},
                    //Query non valida (sintassi)
                    {"SELECT ! FROM testingTable",false, Errors.ResultCode.SYNTAX_ERROR},
                    //Query non valida (parametri)
                    {"INSERT INTO testingTable values (String, 3)",false, Errors.ResultCode.UNDEFINED_COLUMN},
                    //Query vuota
                    {"",false, Errors.ResultCode.INTERNAL_ERROR},
                    //Query null
                    {null,true,null}

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

        @After
        public void tearDownDB()  {

        if(this.queryClient != null)
            this.queryClient.close();

    }
        @Test
        public void executeQueryTest(){
            try{
                //Check if the expected returnCode is equals to the real one
                ClientProtos.SubmitQueryResponse response = this.queryClient.executeQuery(this.query);
                Assertions.assertEquals(this.error,response.getState().getReturnCode());
                System.out.println("Error code" + response.getState().getReturnCode());
            }catch (Exception e){
                System.out.println("Exception has been thrown: "+e.getClass().getName());
                Assertions.assertTrue(this.isExpectedException());
            }
        }

}
