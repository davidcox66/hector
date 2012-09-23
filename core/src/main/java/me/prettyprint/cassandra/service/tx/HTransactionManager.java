package me.prettyprint.cassandra.service.tx;


import java.util.LinkedList;

import me.prettyprint.hector.api.exceptions.HectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages transactions on a per-thread basis where each thread has its own DynamoTransactionManager instance.
 * The transactions within a DynamoTransactionManager are organized as a stack. Each begin of a transaction pushed
 * on the stack and leaving the transaction will pop the latest one from the stack. This push/pop behavior 
 * is regulated by the DynamoTransactionPropagation of the transaction. A new transaction object may or may not 
 * be pushed/popped based upon this policy.
 *
 * The DynamoTransactionManager is also responsible for creating DynamoMutators within the thread through its
 * DynamoMutatorFactory.
 * 
 * @author david
 * @since Jan 4, 2012
 */
public class HTransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger( HTransactionManager.class );
    
    private static ThreadLocal<HTransactionManager> managers = new ThreadLocal<HTransactionManager>();
    private LinkedList<HTransaction> transactions = new LinkedList<HTransaction>();
   
   
    /**
     * Gets the current DynamoTransactionManager associated with this thread, instantiating one if necessary
     * with its default settings.
     */
    public static final HTransactionManager getInstance() {
        HTransactionManager ret = managers.get();
        if( ret == null ) {
           ret = new HTransactionManager();
           managers.set( ret );
        }
        return ret;
    } 

    /**
     * Changes the DynamoTransactionManager for the current thread  
     * @param manager
     */
    public static void setInstance( HTransactionManager manager ) {
        managers.set( manager );
    }

    /**
     * A convenience method to get the current innermost transaction of this current thread.
     * @return
     */
    public static HTransaction getCurrentTransaction() {
        return getInstance().getTransaction();
    } 
    
    HTransactionManager() {
    }
   
    /**
     * Flushes (commits) all the pending changes of the innermost transaction to the database. 
     * This is currently just and  alias for commit() though we could change the semantics in the future.  
     * The intention of this method is to push changes immediately to the database either because you 
     * simply need them there now or are concerned about too many pending mutations in the case of a 
     * large batch of updates.
     */
    public void flush() {
        commit();
    }
    
    /**
     * Flushes (commits) all the pending changes of all transaction in the current thread to the database. 
     * This is currently just and  alias for commit() though we could change the semantics in the future.  
     * The intention of this method is to push changes immediately to the database either because you 
     * simply need them there now or are concerned about too many pending mutations in the case of a 
     * large batch of updates.
     */
    public void flushAll() {
        commitAll();
    }
    
    /**
     * Commits all the pending changes of the innermost transaction to the database. 
     */
    public void commit() {
        if( isInTransaction() ) {
           getTransaction().commit(); 
        }
    }
    
    /**
     * Commits all the pending changes of all transaction in the current thread to the database. 
     */
    public void commitAll() {
        if( isInTransaction() ) {
        	for( HTransaction trans : transactions ) { 
                trans.commit(); 
            }
        }
    }
    
    /**
     * @return true if a transaction has been started in the current thread and we are within 
     * the scope of that transaction. The DynamoTransactionTemplate determines the scope of the
     * transaction.
     */
    public boolean isInTransaction() {
        return transactions.size() > 0;
    }  
   
    /**
     * @return the current innermost transaction of the current thread, throwing and exception if
     * not in a transaction.
     */
    public HTransaction getTransaction() {
        ensureInTransaction();
        return transactions.getLast(); 
    } 
    
    /**
     * Creates a new DynamoTransaction on the transaction stack of the current thread. The thread is
     * assigned the given transactionKey to simply uniquely identify the transaction from others.
     * 
     * @param transactionKey
     * @return
     */
    public HTransaction pushTransaction( Object transactionKey ) {
        HTransaction ret = new HTransaction( this, transactionKey );
        transactions.add( ret );
        return ret;
    }

    /**
     * Pops the innermost transaction for the current thread from the transaction stack, throwing
     * an exception if not in a transaction.    
     */
    public void popTransaction()  {
        ensureInTransaction();
        transactions.removeLast();
    }
  
    /**
     * Validates we are within the scope of a transaction defined by DynamoTransactionTemplate, throwing
     * an exception if not in a transaction. 
     */
    public void ensureInTransaction() {
        if( transactions == null || transactions.size() == 0 ) {
            throw new HectorException( "Not in a transaction");
        }
    } 
}
