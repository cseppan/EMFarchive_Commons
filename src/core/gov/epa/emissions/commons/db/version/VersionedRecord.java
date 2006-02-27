package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DatabaseRecord;

import java.util.ArrayList;
import java.util.List;

public class VersionedRecord extends DatabaseRecord {

    private int recordId;

    private int datasetId;

    private int version;

    private String deleteVersions;

    public VersionedRecord() {// needed for serialization
    }

    public VersionedRecord(int recordId) {
        this.recordId = recordId;
    }

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

    public String[] dataForInsertion(Version version) {
        List data = new ArrayList();

        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version.getVersion() + "");// version
        data.add(3, "");// delete versions

        data.addAll(numVersionCols(), tokensStrings(tokens()));// add all specified data

        return (String[]) data.toArray(new String[0]);
    }

    private List tokensStrings(List tokens) {
        List stringTokens = new ArrayList();
        for (int i = 0; i < tokens.size(); i++) {
            Object object = tokens.get(i);
            String stringValue = (object == null) ? "" : "" + object;
            stringTokens.add(stringValue);
        }
        return stringTokens;
    }

    private int numVersionCols() {
        return 4;
    }

}
