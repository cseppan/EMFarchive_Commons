package gov.epa.emissions.commons.db.version;

public class VersionedRecord {

    private int recordId;

    private int datasetId;

    private int version;

    private String deleteVersions;

    // Record - containing the 'data' columns

    /**
     * @return Returns the deleteVersions.
     */
    public String getDeleteVersions() {
        return deleteVersions;
    }

    /**
     * @param deleteVersions
     *            The deleteVersions to set.
     */
    public void setDeleteVersions(String deleteVersions) {
        this.deleteVersions = deleteVersions;
    }

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

    public int getRecordId() {
        return recordId;
    }

    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }

}
