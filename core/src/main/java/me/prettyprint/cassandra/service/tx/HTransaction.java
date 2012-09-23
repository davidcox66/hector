package me.prettyprint.cassandra.service.tx;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.service.template.AbstractColumnFamilyTemplate;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provides transaction-like behavior for Cassandra updates. This does not imply that DynamoTransaction
 * provides ACID transaction semantics beyond anything provided by Cassandra itself. What is does is
 * provide scope, or grouping, of related mutations to the database. It does this by maintaining 
 * a set of DynamoMutators being used within the current 'transaction'. There is one DynamoMutator per column
 * family. Though there could be one DynamoMutator per key serializer. By column family is more convenient
 * at the moment for tracing/debugging purposes.
 * 
 * Though this does provide ACID transaction semantics, it does represent a consolidation of update
 * logic into a central point of control This may pave the way for further robustness to the updates
 * in lieu of ACID transactions.
 *  
 * @author david
 * @since Jan 4, 2012
 */
public class HTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTransaction.class);
 
	/**
	  * A cache of Mutators keyed by column family name. We retain these so multiple separate updates
	  * to the same column family will use the same mutator without losing previous update requests.
	  */
	private final Map<String,Mutator<?>> mutations = new HashMap<String,Mutator<?>>();
    final HTransactionManager manager;
    
    /**
     * A unique id assigned to this DynamoTransaction by the initiator of the transaction
     */
    final Object transactionKey;
     
    HTransaction( HTransactionManager manager, Object transactionKey ) {
        this.manager = manager;
        this.transactionKey = transactionKey;
    }

    public <K,N> Mutator<K> getMutator( AbstractColumnFamilyTemplate<K,N> template ) {
    	Mutator<K> mutator = (Mutator<K>)mutations.get( template.getColumnFamily() );
    	if( mutator == null ) {
    		mutator = HFactory.createMutator(template.getKeyspace(), template.getKeySerializer());
    		mutations.put( template.getColumnFamily(), mutator );
    	}
    	return mutator;
    }
    
    /**
     * Executes all of the pending mutations for column families within this keyspace
     */
    void commit() {
        long start = 0;
        if( LOGGER.isDebugEnabled() ) { 
            start = System.nanoTime();
            LOGGER.debug( "commit: transationKey=" + transactionKey + ", mutators.size=" + mutations.size() ); 
        } 
        for( Map.Entry<String,Mutator<?>> entry : mutations.entrySet() ) {
            MutationResult result = entry.getValue().execute();
            if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "commit: column family: " + entry.getKey() + " execute time (micro): " + result.getExecutionTimeMicro() ); } 
        }
        if( LOGGER.isDebugEnabled() ) { 
	        long total = (System.nanoTime() - start) / 1000;
	        LOGGER.debug( "commit: execution time (micro): " + total); 
        }
    }
   
    /**
     * Discards all of the pending mutations in all mutators of the current DynamoTransaction 
     */
    void rollback() {
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "rollback: transationKey=" + transactionKey + ", mutators.size=" + mutations.size() ); } 
        mutations.clear();
    }
}
