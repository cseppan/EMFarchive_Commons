package gov.epa.emissions.commons.db.version;


import java.util.ArrayList;
import java.util.List;

public class ChangeSet {

    private List records;

    private Version baseVersion;
    
    public ChangeSet() {
        this.records = new ArrayList();
    }

    public void add(VersionedRecord record) {
        records.add(record);
    }

    public VersionedRecord[] getRecords() {
        return (VersionedRecord[]) records.toArray(new VersionedRecord[0]);
    }

    public Version getBaseVersion() {
        return baseVersion;
    }

    public void setBaseVersion(Version baseVersion) {
        this.baseVersion = baseVersion;
    }

}
