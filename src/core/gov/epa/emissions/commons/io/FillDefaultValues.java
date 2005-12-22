package gov.epa.emissions.commons.io;

import java.util.List;

public class FillDefaultValues {
    private FileFormatWithOptionalCols base;

    public FillDefaultValues(FileFormatWithOptionalCols base) {
        this.base = base;
    }

    public void fillDefaults(List data, long datasetId) {
        data.add(0, datasetId + "");
        addComments(data);
        addDefaultsForOptionalCols(data);
    }

    private void addComments(List data) {
        if (size() == data.size())// includes comments
            return;

        String last = (String) data.get(data.size() - 1);
        if (!isComments(last))
            data.add(data.size(), "!");// empty comment
    }

    private int size() {
        return 1 + base.cols().length + 1;
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
        return 1 + base.minCols().length + optionalCount;// 1 - dataset id
    }

    private int toAdd(int optionalCount) {
        return base.optionalCols().length - optionalCount;
    }

    private int optionalCount(List data) {
        return data.size() - 1 - base.minCols().length - 1;// 1 - dataset id, 1 - comments
    }
}
