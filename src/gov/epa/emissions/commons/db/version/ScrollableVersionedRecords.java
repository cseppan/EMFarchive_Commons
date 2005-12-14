package gov.epa.emissions.commons.db.version;

import java.sql.SQLException;

public interface ScrollableVersionedRecords {

    void execute() throws SQLException;

    int total() throws SQLException;

    int position() throws SQLException;

    void forward(int count) throws SQLException;

    void backward(int count) throws SQLException;

    void moveTo(int index) throws SQLException;

    boolean available() throws SQLException;

    VersionedRecord next() throws SQLException;

    void close() throws SQLException;

    /**
     * @return returns a range of records inclusive of start and end
     */
    VersionedRecord[] range(int start, int end) throws SQLException;

    VersionedRecord[] all() throws SQLException;

}