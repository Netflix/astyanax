package com.netflix.astyanax.ddl;

/**
 * Encapsulates field metadata
 * @author elandau
 *
 */
public class FieldMetadata {
    private final String name;
    private final String type;
    private final boolean isContainer;
    
    public FieldMetadata(String name, String type, boolean isContainer) {
        super();
        this.name = name;
        this.type = type;
        this.isContainer = isContainer;
    }
    
    public String getName() {
        return name;
    }
    
    public String getType() {
        return type;
    }
    
    public boolean isContainer() {
        return this.isContainer;
    }
}
