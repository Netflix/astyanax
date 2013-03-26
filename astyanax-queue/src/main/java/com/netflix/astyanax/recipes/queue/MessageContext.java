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
     * MesasgeID used when acking
     */
    private String ackMessageId;
    
    /**
     * History item associated with this message.  This is only 
     * valid if message.hasKey() is true.
     */
    private MessageHistory history = new MessageHistory();
    
    public Message getMessage() {
        return message;
    }
    
    public MessageContext setMessage(Message message) {
        this.message = message;
        return this;
    }
    
    public Message getNextMessage() {
        return nextMessage;
    }
    
    public MessageContext setNextMessage(Message nextMessage) {
        this.nextMessage = nextMessage;
        return this;
    }

    public MessageHistory getHistory() {
        return history;
    }

    public MessageContext setException(Throwable t) {
        this.history.setStatus(MessageStatus.FAILED);
        this.history.setError(t.getMessage());
        this.history.setStackTrace(ExceptionUtils.getStackTrace(t));
        return this;
    }
 
    public MessageContext setStatus(MessageStatus status) {
        this.history.setStatus(status);
        return this;
    }
    
    public String getAckMessageId() {
        return ackMessageId;
    }

    public MessageContext setAckMessageId(String ackMessageId) {
        this.ackMessageId = ackMessageId;
        return this;
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("MessageContext [")
    	  .append("ackMessageId=" + ackMessageId)
    	  .append(", message=" + message)
    	  .append(", nextMessage=" + nextMessage)
    	  .append("]");
    	
    	return sb.toString();
    }
}
