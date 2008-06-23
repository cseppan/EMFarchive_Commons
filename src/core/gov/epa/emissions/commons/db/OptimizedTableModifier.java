package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;

import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.ParseException;

public class OptimizedTableModifier extends TableModifier {

    private static final int BATCH_SIZE = 20000;

    private int counter;
    
    private int batchCount;

    public OptimizedTableModifier(Datasource datasource, String tableName) throws SQLException {
        super(datasource, tableName);
        counter = 0;
        batchCount = 0;
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
        } catch (Exception e) {
            String msg = e.getMessage();
            String searchString = "Batch entry";
            int line = 0;
            
            if (msg != null && msg.contains(searchString)) {
                msg = msg.substring(searchString.length()).trim(); //msg should start with a number now
                msg = msg.substring(0, msg.indexOf(' ')); //msg should now only has a number
            } 
            
            try {
                line = NumberFormat.getInstance().parse(msg).intValue();
            } catch (ParseException e1) {
                line = 0;
            }
            e.printStackTrace();
            String exmsg = e.getMessage();
            if (e instanceof SQLException)
            {
                SQLException sqle = (SQLException)e;
                if (sqle.getNextException() != null)
                    exmsg = exmsg +"; "+sqle.getMessage();
                    sqle.printStackTrace();
                System.out.println("SQL Exception ERROR: "+exmsg);
            }
            throw new SQLException("Data line (" + (batchCount * BATCH_SIZE + line + 1) + ") has errors: " + exmsg);
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
            batchCount++;
        }
    }
}
