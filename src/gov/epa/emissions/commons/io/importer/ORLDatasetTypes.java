package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class ORLDatasetTypes {

    private Map map;

    public ORLDatasetTypes() {
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
        return new DatasetType("ORL Nonpoint Inventory");
    }

    public DatasetType point() {
        return new DatasetType("ORL Point Inventory");
    }

    public DatasetType onRoad() {
        return new DatasetType("ORL Onroad Inventory");
    }

    public DatasetType nonRoad() {
        return new DatasetType("ORL Nonroad Inventory");
    }

    public DatasetType get(String name) {
        return (DatasetType) map.get(name);
    }
}
