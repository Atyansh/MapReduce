import java.io.*;
import org.apache.hadoop.io.Writable;

public class Ratings implements Writable {
  private int[] ratingCount;

  public Ratings() {
    ratingCount = new int[5];
  }
  
  public void write(DataOutput out) throws IOException {
    for(int i : ratingCount) {
      out.writeInt(i);
    }
  }
  
  public void readFields(DataInput in) throws IOException {
    for(int i = 0; i < ratingCount.length; i++) {
      ratingCount[i] = in.readInt();
    }
  }
  
  public void incr(Ratings ratings) {
    for(int i = 0; i < ratingCount.length; i++) {
      ratingCount[i] += ratings.ratingCount[i];
    }
  }

  public void incr(int rating) {
    ratingCount[rating-1]++;
  }

  public String toString() {
    String ret = "" + ratingCount[0];
    for(int i = 1; i < ratingCount.length; i++) {
      ret += "\t" + ratingCount[i];
    }
    return ret;
  }
}
