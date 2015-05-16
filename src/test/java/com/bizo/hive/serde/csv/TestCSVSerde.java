package com.bizo.hive.serde.csv;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class TestCSVSerde {
  private final CSVSerde csv = new CSVSerde();
  final Properties props = new Properties();
  private static final Logger LOGGER = Logger.getLogger(
      TestCSVSerde.class.getName());

  @Before
  public void setup() {
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string");
  }

  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);

    String csvFile = new String(Files.readAllBytes(Paths
        .get("./src/test/resources/testDeserialize.csv")));

    final Text in = new Text(csvFile);
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<LF>line", row.get(3));
  }

  @Test
  public void testDeserializeSingle() throws Exception {
    csv.initialize(null, props);

    String csvFile = new String(Files.readAllBytes(Paths
        .get("./src/test/resources/testDeserializeSingle.csv")));

    final Text in = new Text(csvFile);
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("no new line", row.get(3));
  }

  @Test
  public void testDeserializeCustomSeparator() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");

    csv.initialize(null, props);

    String csvFile = new String(Files.readAllBytes(Paths
        .get("./src/test/resources/testDeserializeCustomSeparator.csv")));

    final Text in = new Text(csvFile);
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes\tokay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<LF><LF>line", row.get(3));
  }

  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");

    csv.initialize(null, props);

    String csvFile = new String(Files.readAllBytes(Paths
        .get("./src/test/resources/testDeserializeCustomEscape.csv")));

    final Text in = new Text(csvFile);
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes'okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<CRLF>line", row.get(3));
  }

  @Test
  public void testDeserializeCustomSeparatorCustomEscape() throws Exception {
    props.put("seperatorChar", ",");
    props.put("quoteChar", "\"");
    props.put("escapeChar", "\"");

    csv.initialize(null, props);

    String csvFile = new String(Files.readAllBytes(Paths
        .get("./src/test/resources/"
            + "testDeserializeCustomSeparatorCustomEscape.csv")));

    final Text in = new Text(csvFile);
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<CRLF><CRLF>\"line\"", row.get(3));
  }
}