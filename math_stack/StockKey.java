package math_stack;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the symbol. The secondary sort will be performed
 * against the integer input
 */
public class StockKey implements WritableComparable<StockKey> {

   private String symbol;
   private int input;
   
   public StockKey() { }
   
   public StockKey(String symbol, int input) {
      this.symbol = symbol;
      this.input = input;
   }
   
   @Override
   public String toString() {
      return (new StringBuilder())
            .append(symbol)
            .append('_')
            .append(input)
            .toString();
   }
   
   @Override
   public void readFields(DataInput in) throws IOException {
      symbol = WritableUtils.readString(in);
      input = in.readInt();
   }

   @Override
   public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, symbol);
      out.writeInt(input);
   }

   @Override
   public int compareTo(StockKey o) {
      int result = symbol.compareTo(o.symbol);
      
      if(0 == result) {
         if(input < o.input) {
            result = -1;
         } else if(input == o.input) {
            result = 0;
         } else if(input > o.input) {
            result = 1;
         }
      }
      
      return result;
   }

   public String getSymbol() {
      return symbol;
   }

   public void setSymbol(String symbol) {
      this.symbol = symbol;
   }

   public int getInput() {
      return input;
   }

   public void setInput(int input) {
      this.input = input;
   }
}
