package gov.epa.emissions.commons.io.importer.temporal;

public interface ColumnsMetadata {

    int[] widths();

    String[] colTypes();

    String[] colNames();

}