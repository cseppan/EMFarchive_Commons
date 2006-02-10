package gov.epa.emissions.commons.gui;

import java.awt.Component;

import javax.swing.JOptionPane;

public class ConfirmDialog {

    private String message;

    private String title;

    private Component parentWindow;
    
    private CustomDialog dialog;

    public ConfirmDialog(String message, String title, Component parentWindow) {
        this.message = message;
        this.title = title;
        this.parentWindow = parentWindow;

    }
    
    public ConfirmDialog(CustomDialog dialog) {
        this.dialog = dialog;
    }

    public boolean confirm() {
        int option = JOptionPane.showConfirmDialog(parentWindow, message, title, JOptionPane.YES_NO_OPTION);
        return (option == 0);
    }
    
    public boolean confirmYesChoice() {
        int option = dialog.showDialog();
        return (option == JOptionPane.YES_OPTION);
    }

}
