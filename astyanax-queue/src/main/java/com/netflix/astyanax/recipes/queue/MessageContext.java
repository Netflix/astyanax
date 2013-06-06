package com.netflix.astyanax.recipes.queue;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Context of a message being handled by a dispatcher.
 * 
 * @author elandau
 *
 */
public class MessageContext {
    /**
     * Message being handled.  
     * TODO: This should be immutable
     */
    private Message message;
    
    /**
     * Next message that was queued up for processing
     * 
     * TODO:
     */
    private Message nextMessage;
    
    /**
     * MesasgeID used when acking
     */
    private MessageQueueEntry ackEntry;
    
    /**
     * 
     */
    private MessageQueueException error;
    
    /**
     * History item associated with this message.  This is only 
     * valid if message.hasKey() is true.  This is the history item
     * for the current execution only.
     */
    private MessageHistory status = new MessageHistory();

    public MessageContext(MessageQueueEntry messageId, Message message) {
        this.ackEntry = messageId;
        this.message      = message;
    }
    
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
        return status;
    }

    public MessageContext setException(MessageQueueException t) {
        this.error = t;
        this.status.setStatus(MessageStatus.FAILED);
        this.status.setError(t.getMessage());
        this.status.setStackTrace(ExceptionUtils.getStackTrace(t));
        return this;
    }
 
    public MessageContext setStatus(MessageStatus status) {
        this.status.setStatus(status);
        return this;
    }
    
    public MessageQueueEntry getAckQueueEntry() {
        return ackEntry;
    }

    public MessageContext setAckQueueEntry(MessageQueueEntry ackMessageId) {
        this.ackEntry = ackMessageId;
        return this;
    }
    
    public MessageQueueException getError() {
        return this.error;
    }
    
    public boolean hasError() {
        return this.error != null;
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("MessageContext [")
    	  .append("ackMessageId=" + ackEntry)
    	  .append(", message=" + message)
    	  .append(", nextMessage=" + nextMessage)
    	  .append("]");
    	
    	return sb.toString();
    }
}
