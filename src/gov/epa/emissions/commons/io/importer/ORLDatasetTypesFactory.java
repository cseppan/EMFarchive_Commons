package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

public interface ORLDatasetTypesFactory {

    DatasetType nonPoint();

    DatasetType point();

    DatasetType onRoad();

    DatasetType nonRoad();

    DatasetType get(String name);

}