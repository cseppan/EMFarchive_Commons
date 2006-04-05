package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class OptimizedScrollableVersionedRecords implements ScrollableVersionedRecords {

    private static final int FETCH_SIZE = 50000;

    private Datasource datasource;

    private String query;

    private ResultSet resultSet;

    private ResultSetMetaData metadata;

    private int totalCount;

    private int start;

    private Statement statement;

    public OptimizedScrollableVersionedRecords(Datasource datasource, String query, String table, String whereClause)
            throws SQLException {
        this.datasource = datasource;
        this.query = query;
        start = 0;

        obtainTotalCount(table, whereClause);
        executeQuery(start);
    }

    private void obtainTotalCount(String table, String whereClause) throws SQLException {
        Connection connection = datasource.getConnection();
        Statement statement = connection.createStatement();

        String countQuery = "SELECT count(*) FROM " + table + " " + whereClause;
        ResultSet resultSet = statement.executeQuery(countQuery);
        resultSet.next();
        totalCount = resultSet.getInt(1);

        resultSet.close();
    }

    public int total() {
        return totalCount;
    }

    /**
     * @return returns a range of records inclusive of start and end
     */
    public VersionedRecord[] range(int start, int end) throws SQLException {
        moveTo(start);// one position prior to start

        List range = new ArrayList();
        for (int i = start; (i <= end) && (i < totalCount); i++) {
            VersionedRecord record = next();
            if (record != null)
                range.add(record);
        }
        return (VersionedRecord[]) range.toArray(new VersionedRecord[0]);
    }

    private int columnCount() throws SQLException {
        return metadata.getColumnCount();
    }

    public void close() throws SQLException {
        resultSet.close();
        statement.close();
    }

    private void createStatement() throws SQLException {
        Connection connection = datasource.getConnection();
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(FETCH_SIZE);
        statement.setMaxRows(FETCH_SIZE);
    }

    private void executeQuery(int offset) throws SQLException {
        createStatement();

        String currentQuery = query + " LIMIT " + FETCH_SIZE + " OFFSET " + offset;
        resultSet = statement.executeQuery(currentQuery);
        metadata = resultSet.getMetaData();
    }

    private void moveTo(int index) throws SQLException {
        index = positionCursorAt(index);

        if (index == 0)
            resultSet.beforeFirst();
        else
            resultSet.absolute(index);
    }

    private int positionCursorAt(int index) throws SQLException {
        if (inRange(index))
            return index;

        close();
        return moveToNewRange(index);
    }

    private int moveToNewRange(int index) throws SQLException {
        int steps = (index / FETCH_SIZE);
        int stepSize = (steps * FETCH_SIZE);
        start = (index < start) ? (start - stepSize) : (start + stepSize);// move backward/forward direction

        executeQuery(start);
        return index % FETCH_SIZE;// relative index to current range
    }

    private boolean inRange(int index) {
        return (start <= index) && (index < end());
    }

    private int end() {
        return (start + FETCH_SIZE);
    }

    private VersionedRecord next() throws SQLException {
        if (!resultSet.next())
            return null;

        VersionedRecord record = new VersionedRecord();
        record.setRecordId(resultSet.getInt("record_id"));
        record.setDatasetId(resultSet.getInt("dataset_id"));
        record.setVersion(resultSet.getInt("version"));
        record.setDeleteVersions(resultSet.getString("delete_versions"));

        for (int i = 5; i <= columnCount(); i++) {
            record.add(resultSet.getObject(i));
        }
        return record;
    }

}
