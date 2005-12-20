package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DefaultScrollableVersionedRecords implements ScrollableVersionedRecords {
    private Datasource datasource;

    private String query;

    private ResultSet resultSet;

    private ResultSetMetaData metadata;

    public DefaultScrollableVersionedRecords(Datasource datasource, String query) {
        this.datasource = datasource;
        this.query = query;
    }

    public void execute() throws SQLException {
        Connection connection = datasource.getConnection();
        Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

        resultSet = stmt.executeQuery(query);
        metadata = resultSet.getMetaData();
    }

    public int total() throws SQLException {
        int current = position();
        resultSet.last();
        try {
            return position();
        } finally {
            if (current == 0)
                resultSet.beforeFirst();
            else
                resultSet.absolute(current);

        }
    }

    public int position() throws SQLException {
        return resultSet.getRow();
    }

    public void forward(int count) throws SQLException {
        resultSet.relative(count);
    }

    public void backward(int count) throws SQLException {
        resultSet.relative(-count);
    }

    public void moveTo(int index) throws SQLException {
        if (index == 0)
            resultSet.beforeFirst();
        else
            resultSet.absolute(index);
    }

    public boolean available() throws SQLException {
        // TODO: is this a serious hit to the ResultSet's cursor ?
        return position() < total();
    }

    public VersionedRecord next() throws SQLException {
        if (!resultSet.next())
            return null;// TODO: is NullRecord better?

        VersionedRecord record = new VersionedRecord();
        record.setRecordId(resultSet.getInt("record_id"));
        record.setDatasetId(resultSet.getInt("dataset_id"));
        record.setVersion(resultSet.getInt("version"));
        record.setDeleteVersions(resultSet.getString("delete_versions"));

        for (int i = 5; i <= columnCount(); i++)
            record.add(resultSet.getString(i));

        return record;
    }

    private int columnCount() throws SQLException {
        return metadata.getColumnCount();
    }

    public void close() throws SQLException {
        resultSet.close();
    }

    /**
     * @return returns a range of records inclusive of start and end
     */
    public VersionedRecord[] range(int start, int end) throws SQLException {
        moveTo(start);// one position prior to start

        List range = new ArrayList();
        int max = total();
        for (int i = start; (i <= end) && (i < max); i++)
            range.add(next());

        return (VersionedRecord[]) range.toArray(new VersionedRecord[0]);
    }

    public VersionedRecord[] all() throws SQLException {
        return range(0, total());
    }

}
