package gov.epa.emissions.commons.io.importer;

public interface Parser {

    public abstract Record parse(String line);

}