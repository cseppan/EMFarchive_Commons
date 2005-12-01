package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;
import java.util.List;

public class TableDefinitionDelegateTest extends PersistenceTestCase{
    
    private Datasource datasource;
    private SqlDataTypes sqlDataTypes;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
    }
    
    public void testTableExist() throws Exception{
        TableDefinitionDelegate definition = new TableDefinitionDelegate(datasource.getConnection());
        String[] names = {"table1"}; 
        createTables(names);
        assertTrue(definition.tableExist(names[0]));
        dropTables(names);
    }
    
    public void testTableNames() throws Exception{
        TableDefinitionDelegate definition = new TableDefinitionDelegate(datasource.getConnection());
        String[] names = {"nif","orl", "ida"}; 
        createTables(names);
        List tables = definition.getTableNames();
        assertTrue(tables.contains(names[0]));
        assertTrue(tables.contains(names[1]));
        assertTrue(tables.contains(names[2]));
        dropTables(names);
    }

    private void createTables(String[] names) throws SQLException {
        //create table with one column
        Column[] cols = {new Column("col1",sqlDataTypes.stringType(15),new StringFormatter(15))};
        for(int i=0; i<names.length; i++){
            datasource.tableDefinition().createTable(names[i],cols);
        }
    }

    private void dropTables(String[] names) throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        for(int i=0; i< names.length; i++){
            dbUpdate.dropTable(datasource.getName(), names[i]);    
        }
    }
}
