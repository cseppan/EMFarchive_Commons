package gov.epa.emissions.commons.data;

import java.io.Serializable;

public class Reference extends LockableImpl implements Serializable {

    private int id;

    private String description;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "Reference [" + this.id + ", " + this.description + ", " + this.getLockOwner() + ", "
                + this.getLockDate() + "]";
    }
}
