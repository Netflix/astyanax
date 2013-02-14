package com.netflix.astyanax.recipes.queue;

import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * Context of a message being handled by a dispatcher.
 * 
 * @author elandau
 *
 */
public class MessageContext {
    /**
     * Message being handled
     */
    private Message message;
    
    /**
     * Next message that was queued up for processing
     */
    private Message nextMessage;
    
    /**
     * History item associated with this message.  This is only 
     * valid if message.hasKey() is true.
     */
    private MessageHistory history = new MessageHistory();
    
    public Message getMessage() {
        return message;
    }
    
    public void setMessage(Message message) {
        this.message = message;
    }
    
    public Message getNextMessage() {
        return nextMessage;
    }
    
    public void setNextMessage(Message nextMessage) {
        this.nextMessage = nextMessage;
    }

    public MessageHistory getHistory() {
        return history;
    }

    public void setException(Throwable t) {
        this.history.setStatus(MessageStatus.FAILED);
        this.history.setError(t.getMessage());
        this.history.setStackTrace(ExceptionUtils.getStackTrace(t));
    }
 
    public void setStatus(MessageStatus status) {
        this.history.setStatus(status);
    }
    
    @Override
    public String toString() {
        return "MessageContext [message=" + message + ", nextMessage=" + nextMessage + "]";
    }
}
