package math_stack;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Groups values based on symbol of {@link StockKey} (the natural key).
 */
public class KeyGroupingComparator extends WritableComparator {

   /**
    * Constructor.
    */
   protected KeyGroupingComparator() {
      super(StockKey.class, true);
   }
   
   @SuppressWarnings("rawtypes")
   @Override
   public int compare(WritableComparable w1, WritableComparable w2) {
      StockKey k1 = (StockKey)w1;
      StockKey k2 = (StockKey)w2;
      
      return k1.getSymbol().compareTo(k2.getSymbol());
   }
}
