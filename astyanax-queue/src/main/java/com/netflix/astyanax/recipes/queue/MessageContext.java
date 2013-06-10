package com.netflix.astyanax.recipes.queue;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.recipes.queue.entity.MessageHistory;
import com.netflix.astyanax.recipes.queue.entity.MessageQueueEntry;
import com.netflix.astyanax.recipes.queue.exception.MessageQueueException;

/**
 * Context of a message being handled by a dispatcher.
 * 
 * @author elandau
 *
 */
public class MessageContext implements ConsumerMessageContext {
    /**
     * Message being handled.  
     * TODO: This should be immutable
     */
    protected Message message;
    
    /**
     * Next message that was queued up for processing
     * 
     * TODO:
     */
    protected Message nextMessage;
    
    /**
     * MesasgeID used when acking
     */
    protected MessageQueueEntry ackEntry;
    
    /**
     * 
     */
    protected MessageQueueException error;
    
    /**
     * History item associated with this message.  This is only 
     * valid if message.hasKey() is true.  This is the history item
     * for the current execution only.
     */
    protected MessageHistory status = new MessageHistory();

    public MessageContext(MessageQueueEntry messageId, Message message) {
        this.ackEntry = messageId;
        this.message  = message;
    }
    
    @Override
    public Message getMessage() {
        return message;
    }
    
    @Override
    public Message getNextMessage() {
        return nextMessage;
    }
    
    @Override
    public void setNextMessage(Message nextMessage) {
        this.nextMessage = nextMessage;
    }

    public MessageHistory getHistory() {
        return status;
    }

    public MessageContext setStatus(MessageStatus status) {
        this.status.setStatus(status);
        return this;
    }
    
    public MessageQueueEntry getAckQueueEntry() {
        return ackEntry;
    }

    public MessageQueueException getError() {
        return this.error;
    }
    
    public MessageContext setException(MessageQueueException t) {
        this.error = t;
        this.status.setStatus(MessageStatus.FAILED);
        this.status.setError(t.getMessage());
        this.status.setStackTrace(ExceptionUtils.getStackTrace(t));
        return this;
    }

    public MessageContext setAckQueueEntry(MessageQueueEntry ackMessageId) {
        this.ackEntry = ackMessageId;
        return this;
    }
    
//    public MessageContext setMessage(Message message) {
//        Preconditions.checkState(this.message == null, "Once a Message is set on the context it cannot be modified");
//        this.message = message;
//        return this;
//    }
    
    /**
     * Return true if the was an error processing the message.  The error may be set at any state
     * of consuming the message and is used mostly for bulk operations to ignore subsequent 
     * steps in producing or consuming the message.
     * 
     * @return
     */
    public boolean hasError() {
        return this.error != null;
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("MessageContext [")
    	  .append(   "ack="       ).append(ackEntry)
    	  .append(", message="    ).append(message)
    	  .append(", nextMessage=").append(nextMessage)
    	  .append("]");
    	
    	return sb.toString();
    }
}
