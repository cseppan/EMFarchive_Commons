package gov.epa.emissions.commons.io;

public interface TableFormat {

    String key();

    String identify();

    Column[] cols();

}