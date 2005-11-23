package gov.epa.emissions.commons.db.version;

import java.util.ArrayList;
import java.util.List;

public class ChangeSet {

    private List newRecords;

    private Version baseVersion;

    private List deletedRecords;

    public ChangeSet() {
        this.newRecords = new ArrayList();
        this.deletedRecords = new ArrayList();
    }

    public void addNew(VersionedRecord record) {
        newRecords.add(record);
    }

    public VersionedRecord[] getNew() {
        return (VersionedRecord[]) newRecords.toArray(new VersionedRecord[0]);
    }

    public Version getBaseVersion() {
        return baseVersion;
    }

    public void setBaseVersion(Version baseVersion) {
        this.baseVersion = baseVersion;
    }

    public void addDeleted(VersionedRecord record) {
        deletedRecords.add(record);
    }

    public VersionedRecord[] getDeleted() {
        return (VersionedRecord[]) deletedRecords.toArray(new VersionedRecord[0]);
    }

}
