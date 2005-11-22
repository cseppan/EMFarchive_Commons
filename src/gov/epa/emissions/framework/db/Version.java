package gov.epa.emissions.framework.db;

public class Version {

    private int datasetId;

    private int version;

    public int getVersion() {
        return version;
    }

    public void setDatasetId(int datasetId) {
        this.datasetId = datasetId;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getDatasetId() {
        return datasetId;
    }

}
