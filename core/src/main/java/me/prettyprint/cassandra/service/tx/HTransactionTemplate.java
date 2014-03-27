package me.prettyprint.cassandra.service.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This defines the scope of a transaction. Its execute() method is simply passed a closure where
 * the duration of the closure call will occur in a transaction scope. The behavior of the 
 * transaction begin/end will depend on the DynamoTransactionPropagation parameters. This will decide
 * if an existing transaction is required, one will be created only if needed, or one will be 
 * explicitly created.
 * 
 * @author david
 * @since Jan 4, 2012
 */
public class HTransactionTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger( HTransactionTemplate.class );
    
    public static void execute( Runnable closure ) {
        execute( HTransactionPropagation.INHERIT, closure );
    }
    /**
     * Executes the given closure within a transaction scope.
     * 
     * @param propagation
     * @param closure
     */
    public static void execute( HTransactionPropagation propagation, Runnable closure ) {
        // A unique key to give to any DynamoTransaction, if one is instantiated within
        // propagation.enterTransaction(). This will be used later in
        // propagation.exitTransaction() to determine if this DynamoTransactionTemplate
        // had triggered the DynamoTransaction instantiation or if one was inherited.
        Object transactionKey = new Object();
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "execute: entering template transactionKey=" + transactionKey); }
        HTransactionManager manager = HTransactionManager.getInstance();
        HTransaction trans = propagation.enterTransaction( manager, transactionKey );
        boolean finished = false;
        try {
	        closure.run(); 
            finished = true;
        }
        catch( Exception ex ) {
            LOGGER.error( "execute: error durring transaction", ex );
        }
        finally {
            if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "execute: exiting template transactionKey=" + transactionKey + ", finished=" + finished); }
	        propagation.exitTransaction( manager, trans, transactionKey, finished );
        }
    }
}
	