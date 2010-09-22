package gov.epa.emissions.commons.io;

public interface ExportStatement {

    public abstract String generate(String qualifiedTableName, String rowFilters);

}