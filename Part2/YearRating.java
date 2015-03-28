import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class YearRating implements WritableComparable<YearRating> {
  private int year;
  private int rating;
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(year);
    out.writeInt(rating);
  }
  
  public void readFields(DataInput in) throws IOException {
    year = in.readInt();
    rating = in.readInt();
  }
  
  public int compareTo(YearRating o) {
    if(year > o.year) {
      return 1;
    }
    else if(year < o.year) {
      return -1;
    }
    else if(rating > o.rating) {
      return 1;
    }
    else if(rating < o.rating) {
      return -1;
    }

    return 0;
  }

  public void set(int year, int rating) {
    this.year = year;
    this.rating = rating;
  }

  public int getYear() {
    return year;
  }

  public int getRating() {
    return rating;
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + year;
    result = prime * result + rating;
    return result;
  }

  public String toString() {
    return year + "\t" + rating;
  }
}
