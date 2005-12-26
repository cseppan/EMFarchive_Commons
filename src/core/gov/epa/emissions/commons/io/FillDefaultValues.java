package gov.epa.emissions.commons.io;

import java.util.List;

public interface FillDefaultValues {

    void fill(FileFormatWithOptionalCols format, List data, long datasetId);

}