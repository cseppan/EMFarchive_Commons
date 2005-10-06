package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;

import java.util.HashMap;
import java.util.Map;

public class ORLBodyFactory {

    private Map bodyMap;

    ORLBodyFactory(ORLDatasetTypesFactory typesFactory) {
        bodyMap = new HashMap();

        bodyMap.put(typesFactory.nonPoint(), new ORLBody(new NonPointFormatterSequence()));
        bodyMap.put(typesFactory.nonRoad(), new ORLBody(new NonRoadFormatterSequence()));
        bodyMap.put(typesFactory.onRoad(), new ORLBody(new OnRoadMobileFormatterSequence()));
        bodyMap.put(typesFactory.point(), new ORLBody(new PointFormatterSequence()));
    }

    ORLBody getBody(DatasetType type) {
        return (ORLBody) bodyMap.get(type);
    }
}
