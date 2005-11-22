package gov.epa.emissions.framework.db;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class VersionedDataReader {

    private Datasource datasource;

    public VersionedDataReader(Datasource datasource) {
        this.datasource = datasource;
    }

    public VersionedRecord[] fetch(Version version) throws SQLException {
        DataQuery query = datasource.query();
        ResultSet rs = query.executeQuery("SELECT * FROM " + datasource.getName() + ".data WHERE dataset_id = "
                + version.getDatasetId() + " AND version = " + version.getVersion());

        return doFetch(rs);
    }

    // TODO: how does ScrollableRecords fit in here?
    private VersionedRecord[] doFetch(ResultSet rs) throws SQLException {
        List records = new ArrayList();

        while (rs.next()) {
            VersionedRecord record = new VersionedRecord();
            record.setDatasetId(rs.getInt(2));
            record.setVersion(rs.getInt(3));
            
            records.add(record);
        }

        return (VersionedRecord[]) records.toArray(new VersionedRecord[0]);
    }

}
