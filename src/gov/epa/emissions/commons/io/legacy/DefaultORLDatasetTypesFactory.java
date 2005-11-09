package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.DatasetType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class DefaultORLDatasetTypesFactory implements ORLDatasetTypesFactory {

    private Map map;

    public DefaultORLDatasetTypesFactory() {
        map = new HashMap();

        List types = list();
        for (Iterator iter = types.iterator(); iter.hasNext();) {
            DatasetType element = (DatasetType) iter.next();
            map.put(element.getName(), element);
        }
    }

    private List list() {
        List types = new ArrayList();
        types.add(nonPoint());
        types.add(point());
        types.add(nonRoad());
        types.add(onRoad());

        return types;
    }

    public DatasetType nonPoint() {
        return create("ORL Nonpoint Inventory", 8, 12);
    }

    private DatasetType create(String name, int minCols, int maxCols) {
        DatasetType type = new DatasetType(name);
        type.setMinColumns(minCols);
        type.setMaxColumns(maxCols);

        return type;
    }

    public DatasetType point() {
        return create("ORL Point Inventory", 23, 28);
    }

    public DatasetType onRoad() {
        return create("ORL Onroad Inventory", 4, 5);
    }

    public DatasetType nonRoad() {
        return create("ORL Nonroad Inventory", 4, 8);
    }

    public DatasetType get(String name) {
        return (DatasetType) map.get(name);
    }
}
