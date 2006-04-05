package gov.epa.emissions.commons.db.version;

import java.sql.SQLException;

import org.hibernate.Session;

public interface VersionedRecordsReader {

    ScrollableVersionedRecords fetch(Version version, String table, Session session) throws SQLException;

    ScrollableVersionedRecords optimizedFetch(Version version, String table, Session session) throws SQLException;

    ScrollableVersionedRecords fetch(Version version, String table, String columnFilter, String rowFilter,
            String sortOrder, Session session) throws SQLException;

    ScrollableVersionedRecords optimizedFetch(Version version, String table, String columnFilter, String rowFilter,
            String sortOrder, Session session) throws SQLException;
}
