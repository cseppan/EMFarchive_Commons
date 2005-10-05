package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.DefaultORLDatasetTypesFactory;

import java.io.PrintWriter;

//FIXME: fix this mess
public class ORLHeaderWriter {

    /* Header record command fields */
    private static final String COMMAND = "#";

    private static final String ORL_COMMAND = COMMAND + "ORL";

    private static final String TYPE_COMMAND = COMMAND + "TYPE    ";

    private static final String COUNTRY_COMMAND = COMMAND + "COUNTRY ";

    private static final String REGION_COMMAND = COMMAND + "REGION  ";

    private static final String YEAR_COMMAND = COMMAND + "YEAR    ";

    private static final String DESCRIPTION_COMMAND = COMMAND + "DESC    ";

    private DefaultORLDatasetTypesFactory types;

    public ORLHeaderWriter() {
        types = new DefaultORLDatasetTypesFactory();
    }

    void writeHeader(Dataset dataset, PrintWriter writer) {
        String OUT_COMMAND = ORL_COMMAND;
        // FIXME: why is name hard coded ?
        if (dataset.getDatasetType().equals(types.nonPoint())) {
            OUT_COMMAND = ORL_COMMAND + " NONPOINT";
        }
        writer.println(OUT_COMMAND);
        String regionMessage = (dataset.getRegion() != null) ? dataset.getRegion() : " Region not found in database";
        writer.println(REGION_COMMAND + regionMessage);

        String countryMessage = (dataset.getCountry() != null) ? dataset.getCountry()
                : " Country not found in database";
        writer.println(COUNTRY_COMMAND + countryMessage);

        writer.println(YEAR_COMMAND
                + ((dataset.getYear() != 0) ? "" + dataset.getYear() : " Year not found in database"));

        String type = (dataset.getDatasetTypeName() != null) ? dataset.getDatasetTypeName()
                : " Dataset Type not found in database";
        writer.println(TYPE_COMMAND + type);

        writeDescription(dataset.getDescription(), writer);
    }

    private void writeDescription(String description, PrintWriter writer) {
        if (description == null || description.length() == 0)
            writer.println(DESCRIPTION_COMMAND + " Description not found in database");

        String[] descriptions = description.split("\\n");
        for (int i = 0; i < descriptions.length; i++) {
            writer.println(DESCRIPTION_COMMAND + descriptions[i]);
        }

    }
}
