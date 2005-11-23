package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class VersionedRecordsReader {

    private Datasource datasource;

    private VersionsReader versionsReader;

    public VersionedRecordsReader(Datasource datasource) {
        this.datasource = datasource;
        this.versionsReader = new VersionsReader(datasource);
    }

    public VersionedRecord[] fetch(Version version) throws SQLException {
        DataQuery query = datasource.query();

        String versions = fetchCommaSeparatedVersionSequence(version);
        String deleteClause = createDeleteClause(versions);

        ResultSet rs = query.executeQuery("SELECT * FROM " + datasource.getName() + ".data WHERE dataset_id = "
                + version.getDatasetId() + " AND version IN ( " + versions + ") AND " + deleteClause + ") ORDER BY version");

        return doFetch(rs);
    } // TODO: how does ScrollableRecords fit in here?

    private String createDeleteClause(String versions) {
        StringBuffer buffer = new StringBuffer();

        StringTokenizer tokenizer = new StringTokenizer(versions, ",");
        // delete_version NOT SIMILAR TO '(6|6,%|%,6,%|%,6)'
        while (tokenizer.hasMoreTokens()) {
            String version = tokenizer.nextToken();
            String regex = "(" + version + "|" + version + ",%|%," + version + ",%|%," + version + ")";
            buffer.append(" delete_version NOT SIMILAR TO '" + regex + "'");

            if(tokenizer.hasMoreTokens())
                buffer.append(" AND ");
        }

        return buffer.toString();
    }

    private String fetchCommaSeparatedVersionSequence(Version finalVersion) throws SQLException {
        Version[] versions = versionsReader.fetchSequence(finalVersion.getDatasetId(), finalVersion.getVersion());

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < versions.length; i++) {
            result.append(versions[i].getVersion());
            if ((i + 1) < versions.length)
                result.append(",");
        }
        return result.toString();
    }

    private VersionedRecord[] doFetch(ResultSet rs) throws SQLException {
        List records = new ArrayList();

        while (rs.next()) {
            VersionedRecord record = new VersionedRecord();
            record.setRecordId(rs.getInt(1));
            record.setDatasetId(rs.getInt(2));
            record.setVersion(rs.getInt(3));

            records.add(record);
        }

        return (VersionedRecord[]) records.toArray(new VersionedRecord[0]);
    }

}
