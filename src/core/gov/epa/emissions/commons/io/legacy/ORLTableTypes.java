package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.DatasetType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ORLTableTypes {

    private ORLTableType nonPoint;

    private ORLTableType nonRoad;

    private ORLTableType onRoad;

    private ORLTableType point;

    public ORLTableTypes(ORLDatasetTypesFactory typesFactory) {
        nonPoint = new ORLTableType(typesFactory.nonPoint());
        nonRoad = new ORLTableType(typesFactory.nonRoad());
        onRoad = new ORLTableType(typesFactory.onRoad());
        point = new ORLTableType(typesFactory.point());
    }

    private List list() {
        List list = new ArrayList();

        list.add(nonPoint);
        list.add(nonRoad);
        list.add(onRoad);
        list.add(point);

        return list;
    }

    public ORLTableType type(DatasetType datasetType) {
        for (Iterator iter = list().iterator(); iter.hasNext();) {
            ORLTableType type = (ORLTableType) iter.next();
            if (type.datasetType().equals(datasetType))
                return type;
        }

        return null;
    }

    public boolean isNonPoint(String tableType) {
        return nonPoint.base().equals(tableType);
    }

    public boolean isNonRoad(String tableType) {
        return nonRoad.base().equals(tableType);
    }

    public boolean isOnRoad(String tableType) {
        return onRoad.base().equals(tableType);
    }

    public boolean isNonRoad(ORLTableType tableType) {
        return nonRoad.equals(tableType);
    }

    public boolean isOnRoad(ORLTableType tableType) {
        return onRoad.equals(tableType);
    }

    public ORLTableType nonPoint() {
        return nonPoint;
    }

    public ORLTableType onRoad() {
        return onRoad;
    }

    public ORLTableType nonRoad() {
        return nonRoad;
    }

    public ORLTableType point() {
        return point;
    }

    public boolean isPoint(String tableType) {
        return point.base().equals(tableType);
    }

}
