package com.netflix.astyanax.entitystore;

import java.lang.reflect.Method;

import javax.persistence.PostLoad;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;

import com.google.common.base.Preconditions;

public class LifecycleEvents<T> {
    private final Class<T> clazz;
    private Method prePersist;
    private Method postPersist;
    private Method postRemove;
    private Method preRemove;
    private Method postLoad;
    
    public LifecycleEvents(Class<T> clazz) {
        this.clazz = clazz;
        
        for (Method method : this.clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(PrePersist.class)) {
                Preconditions.checkState(prePersist == null, "Duplicate PrePersist annotation on " + method.getName());
                prePersist = method;
                prePersist.setAccessible(true);
            }
            if (method.isAnnotationPresent(PostPersist.class)) {
                Preconditions.checkState(postPersist == null, "Duplicate PostPersist annotation on " + method.getName());
                postPersist = method;
                postPersist.setAccessible(true);
            }
            if (method.isAnnotationPresent(PostRemove.class)) {
                Preconditions.checkState(postRemove == null, "Duplicate PostRemove annotation on " + method.getName());
                postRemove = method;
                postRemove.setAccessible(true);
            }
            if (method.isAnnotationPresent(PreRemove.class)) {
                Preconditions.checkState(preRemove == null, "Duplicate PreRemove annotation on " + method.getName());
                preRemove = method;
                preRemove.setAccessible(true);
            }
            if (method.isAnnotationPresent(PostLoad.class)) {
                Preconditions.checkState(postLoad == null, "Duplicate PostLoad annotation on " + method.getName());
                postLoad = method;
                postLoad.setAccessible(true);
            }
        }
    }

    public void onPrePersist(T obj) throws Exception {
        if (prePersist != null) {
            prePersist.invoke(obj);
        }
    }
    
    public void onPostPersist(T obj) throws Exception {
        if (postPersist != null) {
            postPersist.invoke(obj);
        }
    }
    
    public void onPreRemove(T obj) throws Exception {
        if (preRemove != null) {
            preRemove.invoke(obj);
        }
    }
    
    public void onPostRemove(T obj) throws Exception {
        if (postRemove != null) {
            postRemove.invoke(obj);
        }
    }
    
    public void onPostLoad(T obj) throws Exception {
        if (postLoad != null) {
            postLoad.invoke(obj);
        }
    }
    
}
