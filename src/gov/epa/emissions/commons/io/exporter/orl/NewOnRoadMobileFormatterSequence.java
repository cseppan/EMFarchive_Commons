package gov.epa.emissions.commons.io.exporter.orl;

import java.util.ArrayList;
import java.util.List;

public class NewOnRoadMobileFormatterSequence implements FormatterSequence {

    private List formatters;

    public NewOnRoadMobileFormatterSequence() {
        this.formatters = new ArrayList();

        formatters.add(new FipsFormatter());
        formatters.add(new SccFormatter());
        formatters.add(new NewPollFormatter());
        formatters.add(new AnnEmisFormatter());
        formatters.add(new AvdEmisFormatter());
    }

    public List sequence() {
        return formatters;
    }

}
