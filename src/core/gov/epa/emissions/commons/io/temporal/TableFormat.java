package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.io.Column;

public interface TableFormat {

    String key();

    String identify();

    Column[] cols();

}