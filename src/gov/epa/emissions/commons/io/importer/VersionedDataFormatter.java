package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.version.VersionedRecord;
import gov.epa.emissions.commons.io.Column;

import java.util.ArrayList;
import java.util.List;

public class VersionedDataFormatter {

    private Column[] cols;

    public VersionedDataFormatter(Column[] cols) {
        this.cols = cols;
    }

    private void addComments(List data) {
        if (size() == data.size())// includes comments
            return;

        String last = (String) data.get(data.size() - 1);
        if (!isComments(last))
            data.add(data.size(), "!");// empty comment
    }

    private boolean isComments(String token) {
        return token != null && token.startsWith("!");
    }

    public List format(VersionedRecord record, int version) {
        List data = new ArrayList();

        addVersionData(data, record.getDatasetId(), version);
        data.addAll(numVersionCols(), record.tokens());// add all specified data
        addComments(data);
        dataTokensWithDefaults(record, data);

        return data;
    }

    private void dataTokensWithDefaults(VersionedRecord record, List data) {
        int missing = size() - data.size();
        int index = numVersionCols() + record.tokens().size() - 1;// except comments
        for (int i = 0; i < missing; i++)
            data.add(index + i, "");
    }

    private int size() {
        return cols.length;
    }

    private void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    private int numVersionCols() {
        return 4;
    }

}
