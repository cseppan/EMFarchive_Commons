package gov.epa.emissions.framework.db;

public class VersionedRecord {

    private int version;

    private int datasetId;

    public int getVersion() {
        return version;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(int datasetId) {
        this.datasetId = datasetId;
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
