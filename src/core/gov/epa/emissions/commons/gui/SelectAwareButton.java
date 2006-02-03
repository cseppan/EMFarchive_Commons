package gov.epa.emissions.commons.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Action;

public class SelectAwareButton extends Button {

    private int threshHold;

    private SelectModel selectModel;

    private ConfirmDialog confirmDialog;

    public SelectAwareButton(String label, final Action action, SelectModel model, ConfirmDialog confirmDialog) {
        super(label,action);
        this.selectModel = model;
        this.threshHold = 5;
        this.confirmDialog = confirmDialog;
        
    }

    public SelectAwareButton(String label, final Action action, SelectModel model, int threshHold,
            ConfirmDialog confirmDialog) {
        this(label, action, model, confirmDialog);
        this.threshHold = threshHold;
    }
    
    protected void addActionListener(final Action action){
        addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                if (confirm()) {
                    action.actionPerformed(event);
                }
            }
        });
    }

    protected boolean confirm() {
        int[] selected = selectModel.getSelectedIndexes();
        int noOfSelected = (selected == null) ? 0 : selected.length;
        if (noOfSelected > threshHold) {
            return confirmDialog.confirm();
        }
        return true;
    }
}
