package gov.epa.emissions.framework.db;

import java.sql.Connection;

import junit.framework.Assert;

import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;

public class TableReader extends DbOperation {

    public TableReader(Connection jdbcConnection) {
        super(jdbcConnection);
    }

    public int count(String table) {
        try {
            ITable tableObj = table(table);
            return tableObj.getRowCount();
        } catch (Exception e) {
            throw new RuntimeException("could not query table - " + table);
        }
    }

    private ITable table(String table) {
        try {
            QueryDataSet ds = new QueryDataSet(super.connection);
            ds.addTable(table);

            ITableIterator iterator = ds.iterator();
            Assert.assertTrue("table '" + table + "' does not exist", iterator.next());

            return iterator.getTable();
        } catch (Exception e) {
            throw new RuntimeException("could not lookup table - " + table);
        }
    }

    public boolean exists(String schema, String table) {
        try {
            QueryDataSet ds = new QueryDataSet(super.connection);
            ds.addTable(qualified(schema, table));

            ITableIterator iterator = ds.iterator();
            Assert.assertTrue(iterator.next());
            return iterator.getTable() != null;
        } catch (Exception e) {
            return false;
        }
    }

    public int count(String schema, String table) {
        return count(qualified(schema, table));
    }

    public ITable table(String schema, String table) {
        return table(qualified(schema, table));
    }

    private String qualified(String schema, String table) {
        return schema + "." + table;
    }

}
