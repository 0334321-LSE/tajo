package org.apache.tajo.client;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;

import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;


public class CatalogAdminClientTest extends QueryTestCaseBase {
    //Pattern for db name, now we accept also - _ as special character
    private final String regexPattern = "[^a-zA-Z0-9-_]";
    private final Pattern pattern = Pattern.compile(this.regexPattern);
    private Matcher matcher;
    private final CatalogAdminClient mockedCatalogAdminClient;

    private ArrayList<String> databaseList;

    private static TajoTestingCluster cluster;
    private static TajoClient client;

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
        private  String existingDB;
        private  String createDB;
        private  String existDB;
        private  String dropDB;
        private  boolean existResult;
        private final boolean expectedException;

        private String exceptionClass;

        // Parameters for createDatabase
        public ParameterSet(String existingDB, String createDB, boolean expectedException, String exceptionClass) {
            this.existingDB = existingDB;
            this.createDB = createDB;
            this.expectedException = expectedException;
            this.exceptionClass = exceptionClass;
        }

        // Parameters for existsDatabase
        public ParameterSet(String existingDB, String existDB, boolean existResult, boolean expectedException, String exceptionClass) {
            this.existingDB = existingDB;
            this.existDB = existDB;
            this.existResult = existResult;
            this.expectedException = expectedException;
            this.exceptionClass = exceptionClass;
        }

        // Parameter for dropDatabase
        public ParameterSet(String existingDB, String createDB,String dropDB, boolean expectedException, String exceptionClass) {
            this.existingDB = existingDB;
            this.createDB = createDB;
            this.dropDB = dropDB;
            this.expectedException = expectedException;
            this.exceptionClass = exceptionClass;
        }

        public String getExistingDB() {
            return existingDB;
        }

        public String getCreateDB() {
            return createDB;
        }

        public String getExceptionClass() {
            return exceptionClass;
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
                new ParameterSet("db1","db2",false, null),
                //not valid db name (already exists)
                new ParameterSet("db1","db1",true, DuplicateDatabaseException.class.getName()),
                //empty db name
                new ParameterSet("db1","",true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1",null,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","*$p&cialDB!*",true, Exception.class.getName())
        );
    }
    static Stream<ParameterSet> existsDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db1",true,false, null),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db3",false,false, null),
                //empty db name
                new ParameterSet("db1","",false,true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1",null,false,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","*$p&cialDB!*",false,true, Exception.class.getName())
                );
    }

    static Stream<ParameterSet> dropDatabaseParameters(){
        //Unidimensional selection for existDatabase boundary values
        return Stream.of(
                //valid db name
                new ParameterSet("db1","db2","db1",false, null),
                //not valid db name (db doesn't exists)
                new ParameterSet("db1","db2","db3",true, UndefinedDatabaseException.class.getName()),
                //empty db name
                new ParameterSet("db1","db2","",true, Exception.class.getName()),
                //null db name
                new ParameterSet("db1","db2",null,true, Exception.class.getName()),
                //special char
                new ParameterSet("db1","db2","*$p&cialDB!*",true, Exception.class.getName())
        );
    }


    public CatalogAdminClientTest(){
        MockitoAnnotations.initMocks(this);
        // Mocked catalog admin client
        this.mockedCatalogAdminClient = Mockito.mock(CatalogAdminClient.class);
        this.databaseList = new ArrayList<>();
    }

    @BeforeAll
    public static void setUp() throws Exception {
        cluster = TpchTestBase.getInstance().getTestingCluster();
        client = cluster.newTajoClient();
    }

    /** Define with mock a dummy implementation of create, exists and drop DB. <br />
     * After that, test the real implementation and check if the result are the same.*/
    @BeforeEach
    public void configureMocks() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

        // Mocked CreateDatabase, it does 3 check before the creation
        Mockito.doAnswer(invocation -> {
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
        }).when(mockedCatalogAdminClient).createDatabase(Matchers.anyString());

        /* Mocked existDatabase, it only checks if there is a database with the same name
         of the input argument */
        Mockito.doAnswer(invocation -> {
            String dbName = invocation.getArguments()[0].toString();

            //1)  Check if the dbName string is empty or null
            if( dbName == null || dbName.equals(""))
                throw new IllegalArgumentException("DB name is empty or null");

            //2) Check if the dbName is in the list
            if(this.databaseList.contains(dbName))
                return true;
            else
                return false;
        }).when(mockedCatalogAdminClient).existDatabase(Matchers.anyString());

        // Mocked dropDatabase, it does 3 check before the creation
        Mockito.doAnswer(invocation -> {
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
        }).when(mockedCatalogAdminClient).dropDatabase(Matchers.anyString());
    }


    @ParameterizedTest
    @MethodSource("createDatabaseParameters")
    public void createDatabaseTest(ParameterSet parameterSet){

        boolean isExceptionThrownOne;
        boolean isExceptionThrownTwo;
        Exception e1 = null, e2 = null;

        //CreateDatabase on mocked catalogAdminClient
        try {
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getExistingDB());
            this.mockedCatalogAdminClient.createDatabase(parameterSet.getCreateDB());
            //if expectedException was false, test is gone correctly
            isExceptionThrownOne = false;
            Assertions.assertFalse(parameterSet.expectedException);
        }
        catch (Exception actualE1){
            isExceptionThrownOne = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());

            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())){
                Assertions.assertEquals(parameterSet.getExceptionClass(),actualE1.getClass().getName());
                e1 = actualE1;
            }
        }

        //CreateDatabase on catalogAdminClient implementation
        try {
            client.createDatabase(parameterSet.getExistingDB());
            client.createDatabase(parameterSet.getCreateDB());

            isExceptionThrownTwo = false;
            //if expectedException was false, test is gone correctly
            Assertions.assertFalse(parameterSet.expectedException);
        }
        catch (Exception actualE2){
            isExceptionThrownTwo = true;

            Assertions.assertNotNull(parameterSet.getExceptionClass());
            Assertions.assertTrue(parameterSet.isExpectedException());
            // If it isn't unknown type of exception, check if it is the right one
            if(!parameterSet.getExceptionClass().equals(Exception.class.getName())) {
                Assertions.assertEquals(parameterSet.getExceptionClass(), actualE2.getClass().getName());
                e2 = actualE2;
            }
        }

        Assertions.assertEquals(isExceptionThrownOne,isExceptionThrownTwo,"Both the execution have thrown exception");
        if (e1 != null && e2 != null)
            Assertions.assertEquals(e1.getClass().getName(),e2.getClass().getName(),"Both the executions have thrown the same exception");
        System.out.println("Both the executions have been the same result");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        client.close();
    }

}
