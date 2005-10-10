package gov.epa.emissions.commons.io.importer;

import java.io.IOException;

public interface Reader {

    Record read() throws IOException;

}