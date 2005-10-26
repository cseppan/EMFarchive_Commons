package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

public interface FormatUnit {

    public FileFormat fileFormat();

    public TableFormat tableFormat();
}