package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedTableFormatWithOptionalCols;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DataModifierTest extends PersistenceTestCase {

    protected Datasource datasource;

    protected String table;

    protected void setUp() throws Exception {
        super.setUp();

        datasource = emissions();
        table = "modifier_test";
        createTable(table, datasource);
    }

    private void createTable(String table, Datasource datasource) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, cols());
    }

    private Column[] cols() {
        return tableFormat(dataTypes()).cols();
    }

    protected VersionedTableFormatWithOptionalCols tableFormat(final SqlDataTypes types) {
        FileFormatWithOptionalCols fileFormat = new FileFormatWithOptionalCols() {
            public Column[] optionalCols() {
                return new Column[0];
            }

            public Column[] minCols() {
                Column p1 = new Column("p1", types.text());
                Column p2 = new Column("p2", types.text());

                return new Column[] { p1, p2 };
            }

            public String identify() {
                return "Record_Id";
            }

            public Column[] cols() {
                return minCols();
            }
        };
        return new VersionedTableFormatWithOptionalCols(fileFormat, types);
    }

    protected void tearDown() throws Exception {
        TableDefinition def = datasource.tableDefinition();
        def.dropTable(table);
        super.tearDown();
    }

    public void testShouldInsertRowUsingSpecifiedCols() throws Exception {
        DataModifier modifier = datasource.dataModifier();

        String[] data = { null, "102", "0", "", "p1", "p2" };
        modifier.insertRow(table, data);

        DataQuery query = datasource.query();
        ResultSet rs = query.selectAll(table);
        assertTrue("Should have inserted row", rs.next());
        assertEquals("102", rs.getString(2));
        assertEquals("0", rs.getString(3));
        assertEquals("", rs.getString(4));
        assertEquals("p1", rs.getString(5));
        assertEquals("p2", rs.getString(6));

        rs.close();
    }

    public void testShouldInsertRow() throws Exception {
        DataModifier modifier = datasource.dataModifier();

        String[] data = { null, "102", "0", "", "p1", "p2" };
        modifier.insertRow(table, data);

        DataQuery query = datasource.query();
        ResultSet rs = query.selectAll(table);
        assertTrue("Should have inserted row", rs.next());
        assertEquals("102", rs.getString(2));
        assertEquals("0", rs.getString(3));
        assertEquals("", rs.getString(4));
        assertEquals("p1", rs.getString(5));
        assertEquals("p2", rs.getString(6));

        rs.close();
    }

}
