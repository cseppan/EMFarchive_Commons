package gov.epa.emissions.commons.io.importer;

public interface Parser {

    Record parse(String line);

}