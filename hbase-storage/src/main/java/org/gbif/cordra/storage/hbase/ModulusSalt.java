package org.gbif.cordra.storage.hbase;

import java.nio.charset.Charset;
import org.apache.hadoop.hbase.util.Bytes;

/** Class copied from gbif/maps. TODO: Move to common and use in Spark. */
public class ModulusSalt {
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  private final int modulus;
  private final int digitCount; // used so we get 000, 001, 002 etc
  private final String SEPARATOR = ":";

  public ModulusSalt(int modulus) {
    if (modulus <= 0) {
      throw new IllegalArgumentException("Modulus must be greater than 0");
    }
    this.modulus = modulus;
    digitCount =
        ModulusSalt.digitCount(
            modulus - 1); // minus one because e.g. %100 produces 0..99 (2 digits)
  }

  /** Returns the ID with the salt prefex removed */
  public static String idFrom(byte[] rowKey) {
    try {
      String row = Bytes.toString(rowKey);
      return row.substring(row.indexOf(":") + 1);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Expected key in form of salt:value (e.g. 123:dataset1). Received: "
              + Bytes.toString(rowKey));
    }
  }

  /**
   * Provides the salt from the key or throws IAE.
   *
   * @param key To extract the salt from
   * @return The salt as an integer or throws IAE
   */
  public static int saltFrom(String key) {
    try {
      String saltAsString = key.substring(0, key.indexOf(":"));
      return Integer.parseInt(saltAsString);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Expected key in form of salt:value (e.g. 123:dataset1). Received: " + key);
    }
  }

  public String saltToString(String key) {
    // positive hashcode values only
    int salt = (key.hashCode() & 0xfffffff) % modulus;
    return leftPadZeros(salt, digitCount) + SEPARATOR + key;
  }

  static String leftPadZeros(int number, int length) {
    return String.format("%0" + length + "d", number);
  }

  /**
   * Salts the key ready for use as a byte string in HBase.
   *
   * @param key To salt
   * @return The salted key ready for use with HBase.
   */
  public byte[] salt(String key) {
    return saltToString(key).getBytes(UTF8_CHARSET); // Same implementation as HBase code
  }

  /**
   * Returns the number of digits in the number. This will obly provide sensible results for
   * number>0 and the input is not sanitized.
   *
   * @return the number of digits in the number
   */
  static int digitCount(int number) {
    return (int) (Math.log10(number) + 1);
  }
}
