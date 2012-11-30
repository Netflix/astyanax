package com.netflix.astyanax.recipes.scheduler;

import java.util.concurrent.TimeUnit;

/**
 * Encapsulate the configuration for a task.  Note that the trigger is processed separately
 * so the scheduling can change without having to change or reinsert the task.
 * 
 * @author elandau
 */
public class TaskInfo {
    /**
     * Unique key assigned to the task
     */
    private String  key;
    
    /**
     * Task derived implementation that will be instantiated and executed
     * when the trigger fires
     */
    private String  className;
    
    /**
     * If true then the scheduler will keep track of history for each task execution
     */
    private boolean keepHistory;
    
    /**
     * TTL for history records
     */
    private Integer historyTtl;
    
    /**
     * Builder pattern for creating a TaskInfo
     * @author elandau
     */
    public static class Builder {
        private TaskInfo info = new TaskInfo();
        
        /**
         * Unique key given to the task.
         * @param key
         * @return
         */
        public Builder withKey(String key) {
            info.setKey(key);
            return this;
        }

        /**
         * Class name to execute when the trigger fires
         * @param className
         * @return
         */
        public Builder withClassName(String className) {
            info.setClassName(className);
            return this;
        }
        
        /**
         * Class to execute when the tirgger fires
         * @param clazz
         * @return
         */
        public Builder withClass(Class<?> clazz) {
            info.setClassName(clazz.getCanonicalName());
            return this;
        }
        
        /**
         * Turn on history tracking and specify the TTL 
         * @param ttl
         * @param units
         * @return
         */
        public Builder withHistoryTtl(long ttl, TimeUnit units) {
            info.historyTtl  = (int)TimeUnit.SECONDS.convert(ttl,  units);
            info.keepHistory = true;
            return this;
        }
        
        /**
         * Turn on history without TTL
         * @param keepHistory
         * @return
         */
        public Builder withHistory(boolean keepHistory) {
            info.keepHistory = keepHistory;
            return this;
        }
        
        public TaskInfo build() {
            return info;
        }
    }
    
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public boolean isKeepHistory() {
        return keepHistory;
    }

    public void setKeepHistory(boolean keepHistory) {
        this.keepHistory = keepHistory;
    }

    public Integer getHistoryTtl() {
        return historyTtl;
    }

    public void setHistoryTtl(Integer historyTtl) {
        this.historyTtl = historyTtl;
    }
}
