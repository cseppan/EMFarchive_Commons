package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.DatasetType;

public interface ORLDatasetTypesFactory {

    DatasetType nonPoint();

    DatasetType point();

    DatasetType onRoad();

    DatasetType nonRoad();

    DatasetType get(String name);

    //FIXME: add  isNonPoint ?, isPoint ? and other similar methods
}