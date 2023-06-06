package org.apache.tajo.client;

import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.ipc.ClientProtos;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.util.stream.Stream;

public class QueryClientTest {
    private static TajoTestingCluster cluster;
    private TajoClient client;

    private final String dbName = "TestingDB";
    private final String queryValida = ("INSERT INTO testingTable values (3, 'T');");
    private final String queryNonValida = ("INSERT values (3.5, 'Testing') INTO testingTable;");
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
    }

    static Stream<QueryParameterSet> queryExecuteParameters(){
        return Stream.of(
                //Query valida
                new QueryParameterSet("INSERT INTO testingTable values (3, 'T');",false),
                //Query non valida (sintassi)
                new QueryParameterSet("INSERT values (3, 'T'), INTO testingTable.;",true),
                //Query non valida (parametri)
                new QueryParameterSet("INSERT INTO testingTable values (3.75, 'Testing');",true),
                //Query vuota
                new QueryParameterSet("",true),
                //Query null
                new QueryParameterSet(null,true)
                );
    }
    @ParameterizedTest
    @MethodSource("queryExecuteParameters")
    public void executeQueryTest(QueryParameterSet queryParameterSet){
        try{
            ClientProtos.SubmitQueryResponse response = this.client.executeQuery(queryParameterSet.getQuery());
            TajoIdProtos.QueryIdProto queryIdProto = response.getQueryId();
            QueryId queryId = new QueryId(queryIdProto);
            ResultSet resultSet = this.client.getQueryResult(queryId);

            //If exception was expected, result set must be null (or launch exception)
            if(queryParameterSet.isExpectedException())
                Assertions.assertNull(resultSet);
            else{
                //TODO, modify the query into a select and then check the results
                Assertions.assertNotNull(resultSet);
            }
        }catch (Exception e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(queryParameterSet.isExpectedException());
        }


    }

    @AfterEach
    public void tearDownDB() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
        this.client.dropDatabase(this.dbName);
        if(this.client != null)
            this.client.close();

    }


}
