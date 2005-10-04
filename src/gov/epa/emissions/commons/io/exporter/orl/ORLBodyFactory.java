package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.importer.DefaultORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;

import java.util.HashMap;
import java.util.Map;

public class ORLBodyFactory {

    private Map bodyMap;

    private ORLDatasetTypesFactory types;

    ORLBodyFactory() {
        bodyMap = new HashMap();

        types = new DefaultORLDatasetTypesFactory();
        bodyMap.put(types.nonPoint(), new ORLBody(new NonPointFormatterSequence()));
        bodyMap.put(types.nonRoad(), new ORLBody(new NonRoadFormatterSequence()));
        bodyMap.put(types.onRoad(), new ORLBody(new OnRoadMobileFormatterSequence()));
        bodyMap.put(types.point(), new ORLBody(new PointFormatterSequence()));
    }

    ORLBody getBody(String datasetType) {
        return (ORLBody) bodyMap.get(types.get(datasetType));
    }
}
