package gov.epa.emissions.commons.db.version;

import java.sql.SQLException;

public interface VersionedRecordsReader {

    void close() throws SQLException;

    VersionedRecord[] fetchAll(Version version, String table) throws SQLException;

    VersionedRecord[] fetchAll(Version version, String table, String sortOrder) throws SQLException;

    ScrollableVersionedRecords fetch(Version version, String table) throws SQLException;

    ScrollableVersionedRecords fetch(Version version, String table, String sortOrder) throws SQLException;
}