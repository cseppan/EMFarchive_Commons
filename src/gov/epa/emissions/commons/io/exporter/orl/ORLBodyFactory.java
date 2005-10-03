package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.importer.ORLDatasetTypes;

import java.util.HashMap;
import java.util.Map;

public class ORLBodyFactory {

    private Map bodyMap;

    ORLBodyFactory() {
        bodyMap = new HashMap();

        bodyMap.put(ORLDatasetTypes.NONPOINT.getName(), new ORLBody(new NonPointFormatterSequence()));
        bodyMap.put(ORLDatasetTypes.NONROAD.getName(), new ORLBody(new NonRoadFormatterSequence()));
        bodyMap.put(ORLDatasetTypes.ON_ROAD.getName(), new ORLBody(new OnRoadMobileFormatterSequence()));
        bodyMap.put(ORLDatasetTypes.POINT.getName(), new ORLBody(new PointFormatterSequence()));
    }

    ORLBody getBody(String datasetType) {
        return (ORLBody) bodyMap.get(datasetType);
    }
}
