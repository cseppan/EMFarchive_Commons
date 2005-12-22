package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.util.List;

public interface TableFormatWithOptionalCols extends FileFormatWithOptionalCols, TableFormat {

    void fillDefaults(List data, long datasetId);

}