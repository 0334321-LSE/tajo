package org.apache.tajo.client.queryClient;

import org.apache.tajo.client.QueryClientImpl;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.tajo.client.QueryTestCaseBase.getConf;

@RunWith(Parameterized.class)
public class ExecuteQueryResultTest {

    private QueryClientImpl queryClient;
    private  String query;
    private  boolean expectedException;

    @Parameterized.Parameters
    public static Collection<Object[]> getParametrs(){
        return Arrays.asList(new Object[][]{ //Query valida
                //Query valida
                {"SELECT * FROM testingTable;",false},
                //Query non valida (sintassi)
                {"SELECT ! FROM testingTable",true},
                //Query non valida (parametri)
                {"INSERT INTO testingTable values ('A', 3);",true},
                //Query vuota
                {"",true},
                //Query null
                {null,true}

                // Query for jacoco to reach fetch, block the execution
                // {"SELECT * FROM testingTable WHERE a1 < 3",false}

                // Query for jacoco
                // {"SELECT a1 FROM anotherTable ORDER BY a2",false}
                
        });
    }

    public ExecuteQueryResultTest(String query, boolean expectedException) {
        this.query = query;
        this.expectedException = expectedException;
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
    public void executeQueryResultTest(){
        try{
            ResultSet resultSet = this.queryClient.executeQueryAndGetResult(this.query);
            if(this.expectedException)
                Assertions.fail();

            resultSet.next();
            int resultInteger = resultSet.getInt(1);
            String resultCharacter = resultSet.getString(2);
            boolean control1 = resultInteger == 7;
            boolean control2 = resultCharacter.equals("T");
            Assertions.assertEquals(control1,control2);

        }catch(RuntimeException | TajoInternalError | TajoException | SQLException e){
            System.out.println("Exception has been thrown: "+e.getClass().getName());
            Assertions.assertTrue(this.expectedException);
        }
    }
}
