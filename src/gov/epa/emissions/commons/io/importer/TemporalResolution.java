package gov.epa.emissions.commons.io.importer;

import java.util.List;

/**
 * Type-safe enums for fields (columns) of subsets
 */
public final class TemporalResolution extends Enum {
    public static final TemporalResolution ANNUAL = new TemporalResolution("Annual");

    public static final TemporalResolution MONTHLY = new TemporalResolution("Monthly");

    public static final TemporalResolution WEEKLY = new TemporalResolution("Weekly");

    public static final TemporalResolution DAILY = new TemporalResolution("Daily");

    public static final TemporalResolution HOURLY = new TemporalResolution("Hourly");

    /** List of all NAMES in enumeration. */
    public static final List NAMES = getAllNames(TemporalResolution.class);

    private TemporalResolution(final String name) {
        super(name);
    }

}
