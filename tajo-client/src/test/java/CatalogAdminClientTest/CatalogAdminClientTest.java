package CatalogAdminClientTest;

import org.apache.tajo.client.*;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


import static org.mockito.Mockito.*;

public class CatalogAdminClientTest {
    //Pattern for db name, now we accept also - _ as special character
    private final String regexPattern = "[^a-zA-Z0-9-_]";
    private final Pattern pattern = Pattern.compile(this.regexPattern);
    private Matcher matcher;
    //private final TajoClientImpl tajoClient;

    private CatalogAdminClient catalogAdminClient;

    //private final TajoClientImpl mockedTajoClient;

    private CatalogAdminClient mockedCatalogAdminClient;

    private ArrayList<String> databaseList;

/*    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                // Test some flow of executions, unidimensional selection:
                // at least  1 test case for each boundary value
                //CREATE DB
                // createDB_1   createDB_2  existDB_1   dropDB_2    exist_result    exception
            *//*0*//*{ "db1",        "db2",      "db1",      "db2",      true,          false},
            *//*1*//*{ "db1",        "db1",      "db1",      "db2",      true,          false}


        });
    }*/

    static class ParameterSet {
        private final String existingDB;
        private final String createDB;
        private final String existDB;
        private final String dropDB;

        private final boolean existResult;
        private final boolean expectedException;

        public ParameterSet(String existingDB, String createDB, String existDB, String dropDB, boolean existResult, boolean expectedException) {
            this.existingDB = existingDB;
            this.createDB = createDB;
            this.existDB = existDB;
            this.dropDB = dropDB;
            this.existResult = existResult;
            this.expectedException = expectedException;
        }

        public String getExistingDB() {
            return existingDB;
        }

        public String getCreateDB() {
            return createDB;
        }

        public String getExistDB() {
            return existDB;
        }

        public String getDropDB() {
            return dropDB;
        }

        public boolean isExistResult() {
            return existResult;
        }

        public boolean isExpectedException() {
            return expectedException;
        }
    }

    static Stream<ParameterSet> createDatabaseParameters(){
        //Unidimensional selection for createDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2","db1","db2",true,false),
                //not valid db name (already exists)
                new ParameterSet("db1","db1","db1","db2",true,true),
                //empty db name
                new ParameterSet("db1","","db1","db2",true,true),
                //null db name
                new ParameterSet("db1",null,"db1","db2",true,true),
                //special char
                new ParameterSet("db1","*$p&cialDB!*","db1","db2",true,true)
        );
    }
    static Stream<ParameterSet> existsDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2","db1","db2",true,false),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db2","db3","db2",false,false),
                //empty db name
                new ParameterSet("db1","db2","","db2",false,true),
                //null db name
                new ParameterSet("db1","db2",null,"db2",false,true),
                //special char
                new ParameterSet("db1","db2","*$p&cialDB!*","db2",false,true)
                );
    }

    static Stream<ParameterSet> dropDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2","db1","db2",true,false),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db2","db3","db3",false,true),
                //empty db name
                new ParameterSet("db1","db2","db1","",true,true),
                //null db name
                new ParameterSet("db1","db2","db3",null,false,true),
                //special char
                new ParameterSet("db1","db2","db3","*$p&cialDB!*",false,true)
        );
    }


    public CatalogAdminClientTest(){
        ServiceTracker serviceTracker = new DummyServiceTracker(mock(InetSocketAddress.class));

        // Real implementation ov catalog admin client
        this.catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(serviceTracker,null,new KeyValueSet()));
        //this.tajoClient = new TajoClientImpl(serviceTracker);

        // Mocked catalog admin client
        this.mockedCatalogAdminClient = mock(CatalogAdminClient.class);
        //this.mockedTajoClient = new TajoClientImpl(serviceTracker);

        this.databaseList = new ArrayList<>();
    }

    /** Define with mock a dummy implementation of create, exists and drop DB. <br />
     * After that, test the real implementation and check if the result are the same.*/
    @BeforeClass
    public void setUp() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

        // Mocked CreateDatabase, it does 3 check before the creation
        doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

           //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName contains special characters ( expected _ or -)
            this.matcher = this.pattern.matcher(dbName);
            if (this.matcher.find())
                throw new IllegalArgumentException("DB name contains special characters .");

            //3) Check if dbName already exists
            if (this.databaseList.contains(dbName))
                throw new DuplicateDatabaseException("DB name already exists");
            else
                //Creation simply is represented by adding the db to an array list
                this.databaseList.add(dbName);

            return null;
        }).when(mockedCatalogAdminClient).createDatabase(anyString());

        /* Mocked existDatabase, it only checks if there is a database with the same name
         of the input argument */
        doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName is in the list
            if(this.databaseList.contains(dbName))
                return true;
            else
                return false;
        }).when(mockedCatalogAdminClient).existDatabase(anyString());

        // Mocked dropDatabase, it does 3 check before the creation
        doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName contains special characters ( expected _ or -)
            this.matcher = this.pattern.matcher(dbName);
            if (this.matcher.find())
                throw new IllegalArgumentException("DB name contains special characters .");

            //3) Check if dbName not exists in the list
            if (!this.databaseList.contains(dbName))
                throw new UndefinedDatabaseException("DB name doesn't exist");
            else
                //Creation simply is represented by adding the db to an array list
                this.databaseList.remove(dbName);

            return null;
        }).when(mockedCatalogAdminClient).dropDatabase(anyString());
    }


    @ParameterizedTest
    @MethodSource("createDatabaseParameters")
    public void createDatabaseTest(ParameterSet parameterSet) throws DuplicateDatabaseException {
        //Try createDatabase on both mocked and regular catalogAdminClient
        try {
            this.catalogAdminClient.createDatabase(parameterSet.getExistingDB());
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getExistingDB());
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(parameterSet.expectedException);
        }
        catch (Exception e){
            System.out.println("Caught exception "+e.getClass().getName()+ "\n Expectations "+parameterSet.isExpectedException());
            Assertions.assertTrue(parameterSet.isExpectedException(), "Caught exception: "+e.getClass().getName());
        }
    }
}
