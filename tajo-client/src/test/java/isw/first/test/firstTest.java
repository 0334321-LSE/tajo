package isw.first.test;
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
import org.apache.tajo.client.CatalogAdminClientImpl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import static org.mockito.Mockito.when;

/** Those tests are first test made only to play a little bit with mockito and jUnit */


public class firstTest {

    private static final String MOCKITO_LABEL = "Mockito test, let's get it !";
    private static final String DB_LABEL = " exist";

    @Mock
    CatalogAdminClientImpl catalogAdminClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        final String dbName2 = "DB_pazzo";
        final String dbName = "DB_serio";
        // add the mocked behavior for two DB names
        when(catalogAdminClient.existDatabase(dbName2)).thenReturn(true);
        when(catalogAdminClient.existDatabase(dbName)).thenReturn(true);
    }

    @Test
    public void testOne(){
        Assertions.assertTrue(true);
    }

    @Test
    public void mockitoTest(){
        String dbN = "DB_pazzo";
        String dbN2 = "DB_serio";

        boolean b1 = catalogAdminClient.existDatabase(dbN);
        boolean b2 = catalogAdminClient.existDatabase(dbN2);
        boolean b3 = catalogAdminClient.existDatabase("DB_pippo");

        boolean condition = b1 && b2 && !b3;
        Assertions.assertTrue(condition);
    }

}
