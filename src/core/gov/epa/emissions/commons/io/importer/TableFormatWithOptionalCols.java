package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.util.List;

public interface TableFormatWithOptionalCols extends FileFormatWithOptionalCols, TableFormat {

    String key();

    Column[] cols();

    void fillDefaults(List data, long datasetId);

    String identify();

    Column[] optionalCols();

    Column[] minCols();

}