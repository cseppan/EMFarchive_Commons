package gov.epa.emissions.commons.io;

import java.util.List;

public class FillDefaultValuesOfVersionedRecord {
    private FileFormatWithOptionalCols base;

    public FillDefaultValuesOfVersionedRecord(FileFormatWithOptionalCols base) {
        this.base = base;
    }

    public void fillDefaults(List data, long datasetId) {
        addVersionData(data, datasetId, 0);
        addComments(data);
        addDefaultsForOptionalCols(data);
    }

    private void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    private void addComments(List data) {
        if (size() == data.size())// includes comments
            return;

        String last = (String) data.get(data.size() - 1);
        if (!isComments(last))
            data.add(data.size(), "!");// empty comment
    }

    private int size() {
        return versionColsCount() + base.cols().length + 1;
    }

    private int versionColsCount() {
        return 4;
    }

    private boolean isComments(String token) {
        return token != null && token.startsWith("!");
    }

    /**
     * pre-condition: dataset id and comments are filled in
     */
    private void addDefaultsForOptionalCols(List data) {
        int optionalCount = optionalCount(data);
        int toAdd = toAdd(optionalCount);
        int insertAt = insertAt(optionalCount);

        for (int i = 0; i < toAdd; i++)
            data.add(insertAt + i, "");// fillers for missing optional cols
    }

    private int insertAt(int optionalCount) {
        return versionColsCount() + base.minCols().length + optionalCount;
    }

    private int toAdd(int optionalCount) {
        return base.optionalCols().length - optionalCount;
    }

    private int optionalCount(List data) {
        return data.size() - versionColsCount() - base.minCols().length - 1;// 1 - comments
    }
}
