package gov.epa.emissions.commons.gui;

import java.util.ArrayList;
import java.util.List;

public class ChangeablesList {
    private List listOfChangeables;
    
    private ChangeObserver window;
    
    public ChangeablesList(ChangeObserver window) {
        this.window = window;
        listOfChangeables = new ArrayList();
    }
    
    public void add(Changeable changeable) {
        changeable.observe(this);
        listOfChangeables.add(changeable);
    }
    
    public void add(List changeables) {
        for(int i = 0; i < changeables.size(); i++)
            add((Changeable)changeables.get(i));
    }
    
    public boolean hasChanges() {
        for(int i = 0; i < listOfChangeables.size(); i++) 
            if(query((Changeable)listOfChangeables.get(i)))
                return true;
        
        return false;
    }
    
    private boolean query(Changeable c) {
        return c.hasChanges();
    }
    
    public void onChanges() {
        window.onChanges();
    }
    
}
