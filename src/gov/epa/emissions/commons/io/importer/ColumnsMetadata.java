package gov.epa.emissions.commons.io.importer;

public interface ColumnsMetadata {

    int[] widths();

    String[] colTypes();

    String[] colNames();

}