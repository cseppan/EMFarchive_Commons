package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;

import java.sql.SQLException;

public class OptimizedTableModifier extends TableModifier {

    private static final int BATCH_SIZE = 20000;

    private int counter;

    public OptimizedTableModifier(Datasource datasource, String tableName) throws SQLException {
        super(datasource, tableName);
        counter = 0;
    }

    public void start() throws SQLException {
        connection.setAutoCommit(false);
        //System.out.println("Batch size = "+BATCH_SIZE);
    }

    public void insert(String[] data) throws Exception {
        if (data.length > columns.length) {
            throw new Exception("Invalid number of data tokens - " + data.length + ". Number of columns in the table: "
                    + columns.length);
        }
        insertRow(tableName, data, columns);
    }

    public void finish() throws SQLException {
        try {
            statement.executeBatch();// executing the last batch
        } finally {
            connection.commit();
            connection.setAutoCommit(true);
        }
    }

    private void insertRow(String table, String[] data, Column[] cols) throws SQLException {
        StringBuffer insert = createInsertStatement(table, data, cols);
        try {
            execute(insert.toString());
        } catch (SQLException e) {
            connection.rollback();
            connection.setAutoCommit(true);
            throw e;
        }
    }

    private void execute(String query) throws SQLException {
        if (counter < BATCH_SIZE) {
            statement.addBatch(query);
            counter++;
        } else {
            statement.addBatch(query);
            statement.executeBatch();
            counter = 0;
        }
    }
}
