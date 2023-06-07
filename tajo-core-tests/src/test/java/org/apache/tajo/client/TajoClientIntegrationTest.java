package org.apache.tajo.client;

import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.apache.tajo.QueryTestCaseBase.getConf;
import static org.mockito.Matchers.any;
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
        doNothing().when(this.queryClient).executeQuery(anyString());
        this.tajoClient.executeQuery(createTable);
        verify(this.tajoClient, times(1)).executeQuery(createTable);


        //Now check if is correctly called executeQueryAndGetResult
        doNothing().when(this.queryClient).executeQueryAndGetResult(anyString());
        this.tajoClient.executeQueryAndGetResult(createTable);
        verify(this.tajoClient, times(1)).executeQueryAndGetResult(createTable);

        //Check if is correctly called one time existDB
        doNothing().when(this.catalogAdminClient).existDatabase(anyString());
        boolean exists = this.tajoClient.existDatabase(dbName);
        verify(this.catalogAdminClient, times(1)).existDatabase(dbName);

        //Check if is correctly called one time dropDB
        doNothing().when(this.catalogAdminClient).dropDatabase(anyString());
        this.tajoClient.dropDatabase(dbName);
        verify(this.catalogAdminClient, times(1)).dropDatabase(dbName);
    }
}
