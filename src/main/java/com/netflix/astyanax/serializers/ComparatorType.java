package com.netflix.astyanax.serializers;

/**
 * @author: peter
 */
public class ComparatorType {

  public static ComparatorType ASCIITYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.AsciiType");
  public static ComparatorType BYTESTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.BytesType");
  public static ComparatorType INTEGERTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.IntegerType");
  public static ComparatorType LEXICALUUIDTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.LexicalUUIDType");
  public static ComparatorType LOCALBYPARTITIONERTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.LocalByPartionerType");
  public static ComparatorType LONGTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.LongType");
  public static ComparatorType TIMEUUIDTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.TimeUUIDType");
  public static ComparatorType UTF8TYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.UTF8Type");
  public static ComparatorType COMPOSITETYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.CompositeType");
  public static ComparatorType DYNAMICCOMPOSITETYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.DynamicCompositeType");
  public static ComparatorType UUIDTYPE = new ComparatorType(
      "org.apache.cassandra.db.marshal.UUIDType");

  private static ComparatorType[] values = { ASCIITYPE, BYTESTYPE, INTEGERTYPE,
      LEXICALUUIDTYPE, LOCALBYPARTITIONERTYPE, LONGTYPE, TIMEUUIDTYPE,
      UTF8TYPE, COMPOSITETYPE, DYNAMICCOMPOSITETYPE, UUIDTYPE };

  private final String className;
  private final String typeName;

  private ComparatorType(String className) {
    this.className = className;
    if (className.startsWith("org.apache.cassandra.db.marshal.")) {
      typeName = className.substring("org.apache.cassandra.db.marshal."
          .length());
    } else {
      typeName = className;
    }
  }

  public String getClassName() {
    return className;
  }

  public String getTypeName() {
    return typeName;
  }

  public static ComparatorType getByClassName(String className) {
    if (className == null) {
      return null;
    }
    for (int a = 0; a < values.length; a++) {
      ComparatorType type = values[a];
      if (type.getClassName().equals(className)) {
        return type;
      }
      if (type.getClassName().equals(
          "org.apache.cassandra.db.marshal." + className)) {
        return type;
      }
    }
    return new ComparatorType(className);
  }
}
