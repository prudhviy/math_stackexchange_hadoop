package math_stack;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compares the composite key, {@link StockKey}.
 * We sort by symbol ascendingly and input ascendingly
 */

public class CompositeKeyComparator extends WritableComparator {

   /**
    * Constructor.
    */
   protected CompositeKeyComparator() {
      super(StockKey.class, true);
   }
   
   @SuppressWarnings("rawtypes")
   @Override
   public int compare(WritableComparable w1, WritableComparable w2) {
      StockKey k1 = (StockKey)w1;
      StockKey k2 = (StockKey)w2;
      
      int result = k1.getSymbol().compareTo(k2.getSymbol());
      
      if(0 == result) {
         if(k1.getInput() < k2.getInput()) {
            result = -1;
         } else if(k1.getInput() == k2.getInput()) {
            result = 0;
         } else if(k1.getInput() > k2.getInput()) {
            result = 1;
         }
      }
      
      return result;
   }
}
