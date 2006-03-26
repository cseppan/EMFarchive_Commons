package gov.epa.emissions.commons.gui;

import java.util.List;

public interface Changeables {

    public abstract void add(Changeable changeable);

    public abstract void add(List changeables);

    public abstract boolean hasChanges();

    public abstract void resetChanges();

    public abstract void onChanges();

}