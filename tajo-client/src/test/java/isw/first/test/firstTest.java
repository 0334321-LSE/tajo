package isw.first.test;

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
