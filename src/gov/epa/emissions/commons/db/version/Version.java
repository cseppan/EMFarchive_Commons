package gov.epa.emissions.commons.db.version;

public class Version {

    private int datasetId;

    private int version;

    private String path;

    private boolean finalVersion = false;

    public boolean isFinalVersion() {
        return finalVersion;
    }

    public void markFinal() {
        this.finalVersion = true;
    }

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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFinalVersion(boolean finalVersion) {
        this.finalVersion = finalVersion;
    }
}
