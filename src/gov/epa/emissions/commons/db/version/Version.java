package gov.epa.emissions.commons.db.version;

public class Version {

    private long datasetId;

    private int version;

    private String path;

    private boolean finalVersion = false;

    private String name;

    public boolean isFinalVersion() {
        return finalVersion;
    }

    public void markFinal() {
        this.finalVersion = true;
    }

    public int getVersion() {
        return version;
    }

    public void setDatasetId(long datasetId) {
        this.datasetId = datasetId;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getDatasetId() {
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

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
