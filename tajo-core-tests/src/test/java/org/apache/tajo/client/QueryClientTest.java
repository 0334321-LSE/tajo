package org.apache.tajo.client;

import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.*;
import org.apache.tajo.ipc.ClientProtos;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

public class QueryClientTest {
    private static TajoTestingCluster cluster;
    private TajoClient client;

    private final String dbName = "TestingDB";

    @BeforeAll
    public static void setUp() {
        cluster = TpchTestBase.getInstance().getTestingCluster();
    }
    @BeforeEach
    public void createDB() throws Exception {
        this.client = cluster.newTajoClient();
        this.client.createDatabase(this.dbName);
        this.client.selectDatabase(this.dbName);
        String createTable = ("CREATE TABLE TestingTable (a1 int, a2 char);");
        this.client.executeQuery(createTable);
        String insertInto ="INSERT INTO testingTable values (7, 'T');";
        this.client.executeQuery(insertInto);
    }

    static Stream<QueryParameterSet> executeQueryParameters(){
        return Stream.of(
                //Query valida
                new QueryParameterSet("SELECT * FROM testingTable;",false, Errors.ResultCode.OK),
                //Query non valida (sintassi)
                new QueryParameterSet("SELECT ! FROM testingTable",true, Errors.ResultCode.SYNTAX_ERROR),
                //Query non valida (parametri)
                new QueryParameterSet("INSERT INTO testingTable values (String, 3)",true, Errors.ResultCode.UNDEFINED_COLUMN),
                //Query vuota
                new QueryParameterSet("",true, Errors.ResultCode.INTERNAL_ERROR),
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
        );
    }

    @ParameterizedTest
    @MethodSource("executeQueryParameters")
    public void executeQueryTest(QueryParameterSet queryParameterSet){
        try{
            //Check if the expected returnCode is equals to the real one
            ClientProtos.SubmitQueryResponse response = this.client.executeQuery(queryParameterSet.getQuery());
            Assertions.assertEquals(queryParameterSet.getError(),response.getState().getReturnCode());

            //TODO also try to check the response on first case
            /*TajoIdProtos.QueryIdProto queryIdProto = response.getQueryId();
            QueryId queryId = new QueryId(queryIdProto);
            ResultSet resultSet = this.client.getQueryResult(queryId);
*//*
            //If exception was expected, result set must be null (or launch exception)
            if(queryParameterSet.isExpectedException())
                Assertions.assertNull(resultSet);
            else{
                Assertions.assertNotNull(resultSet);
                resultSet.next();
                int resultInteger = resultSet.getInt(1);
                String resultCharacter = resultSet.getString(2);
                boolean control1 = resultInteger == 7;
                boolean control2 = resultCharacter.equals("T");
                Assertions.assertEquals(control2,control1);

            }*/
        }catch (Exception e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        }
    }

    @ParameterizedTest
    @MethodSource("executeQueryAndGetResultParameters")
    public void executeAndGetResultTest(QueryParameterSet queryParameterSet){
        try{
            ResultSet resultSet = this.client.executeQueryAndGetResult(queryParameterSet.getQuery());
            if(queryParameterSet.isExpectedException())
                Assertions.fail();

            resultSet.next();
            int resultInteger = resultSet.getInt(1);
            String resultCharacter = resultSet.getString(2);
            boolean control1 = resultInteger == 7;
            boolean control2 = resultCharacter.equals("T");
            Assertions.assertEquals(control1,control2);

        }catch(RuntimeException | TajoInternalError | TajoException e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        } catch (SQLException e) {
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        }

    }

    @AfterEach
    public void tearDownDB() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
        this.client.selectDatabase("default");
        this.client.dropDatabase(this.dbName);
        if(this.client != null)
            this.client.close();

    }


}
