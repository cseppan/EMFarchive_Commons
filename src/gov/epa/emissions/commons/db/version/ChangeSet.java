package gov.epa.emissions.commons.db.version;

import java.util.ArrayList;
import java.util.List;

public class ChangeSet {

    private List newRecords;

    private List deletedRecords;

    private List updatedRecords;

    private Version version;

    public ChangeSet() {
        this.newRecords = new ArrayList();
        this.deletedRecords = new ArrayList();
        this.updatedRecords = new ArrayList();
    }

    public void addNew(VersionedRecord record) {
        newRecords.add(record);
    }

    public VersionedRecord[] getNew() {
        return (VersionedRecord[]) newRecords.toArray(new VersionedRecord[0]);
    }

    public void addDeleted(VersionedRecord record) {
        deletedRecords.add(record);
    }

    public VersionedRecord[] getDeleted() {
        return (VersionedRecord[]) deletedRecords.toArray(new VersionedRecord[0]);
    }

    public void addUpdated(VersionedRecord record) {
        updatedRecords.add(record);
    }

    public VersionedRecord[] getUpdated() {
        return (VersionedRecord[]) updatedRecords.toArray(new VersionedRecord[0]);
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

}
