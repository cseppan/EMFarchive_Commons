package gov.epa.emissions.commons.db.version;

import java.sql.SQLException;

import org.hibernate.Session;

public interface VersionedRecordsReader {

    VersionedRecord[] fetchAll(Version version, String table, Session session) throws SQLException;

    VersionedRecord[] fetchAll(Version version, String table, String sortOrder, Session session) throws SQLException;

    ScrollableVersionedRecords fetch(Version version, String table, Session session) throws SQLException;

    ScrollableVersionedRecords fetch(Version version, String table, String sortOrder, Session session)
            throws SQLException;
}