package gov.epa.emissions.commons.db.version;

public class Version {

    private int datasetId;

    private int version;
    
    private String parentVersions;

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

    public String getParentVersions() {
        return parentVersions;
    }

    public void setParentVersions(String parentVers){
        parentVersions=parentVers;
    }
}
