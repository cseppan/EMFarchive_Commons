package gov.epa.emissions.commons.io.importer;

import java.io.IOException;
import java.util.List;

public interface Reader {

    Record read() throws IOException;

    List comments();

}