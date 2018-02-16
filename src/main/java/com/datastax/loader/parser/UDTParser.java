package com.datastax.loader.parser;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.StringReader;
import java.text.ParseException;
import java.util.UUID;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.reflect.TypeToken;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class UDTParser extends AbstractParser {
  private StringParser fieldNameParser = new StringParser();
  private List<Parser> fieldValueParsers;
  private char collectionDelim;
  private char collectionBegin;
  private char collectionEnd;
  private char collectionQuote = '\"';
  private char collectionEscape = '\\';
  private char mapDelim;
  private String collectionNullString = null;
  private UserType userType;
  private UDTValue udtValue;

  private CsvParser csvp = null;

  public UDTParser(List<Parser> inElemParsers, UserType inUserType,
                   char inCollectionDelim, char inCollectionBegin,
                   char inCollectionEnd, char inMapDelim) {
    fieldValueParsers = inElemParsers;
    collectionDelim = inCollectionDelim;
    collectionBegin = inCollectionBegin;
    collectionEnd = inCollectionEnd;
    mapDelim = inMapDelim;
    userType = inUserType;
    udtValue = userType.newValue();

    CsvParserSettings settings = new CsvParserSettings();
    settings.getFormat().setLineSeparator("" + collectionDelim);
    settings.getFormat().setNormalizedNewline(collectionDelim);
    settings.getFormat().setDelimiter(mapDelim);
    settings.getFormat().setQuote(collectionQuote);
    settings.getFormat().setQuoteEscape(collectionEscape);
    settings.getFormat().setCharToEscapeQuoteEscaping(collectionEscape);
    settings.setKeepQuotes(true);
    settings.setKeepEscapeSequences(true);

    csvp = new CsvParser(settings);
  }
  public Object parseIt(String toparse) throws ParseException {
    if (null == toparse)
      return null;
    if (!toparse.startsWith(Character.toString(collectionBegin)))
      throw new ParseException("Must begin with " + collectionBegin
          + "\n", 0);
    if (!toparse.endsWith(Character.toString(collectionEnd)))
      throw new ParseException("Must end with " + collectionEnd
          + "\n", 0);
    toparse = toparse.substring(1, toparse.length() - 1);
    StringReader sr = new StringReader(toparse);
    csvp.beginParsing(sr);
    udtValue = userType.newValue();
    try {
      String[] row;
      int i = 0;
      while ((row = csvp.parseNext()) != null) {
        String key = (String) fieldNameParser.parse(row[0]);
        Object value = fieldValueParsers.get(i).parse(row[1]);
        if (value != null) {
          switch (userType.getFieldType(key).getName()) {
            case BOOLEAN:
              udtValue.setBool(key, (Boolean) value);
              break;

            case TINYINT:
              udtValue.setByte(key, (Byte) value);
              break;
            case SMALLINT:
              udtValue.setShort(key, (Short) value);
              break;
            case INT:
              udtValue.setInt(key, (Integer) value);
              break;
            case BIGINT:
              udtValue.setLong(key, (Long) value);
              break;

            case FLOAT:
              udtValue.setFloat(key, (Float) value);
              break;
            case DOUBLE:
              udtValue.setDouble(key, (Double) value);
              break;

            case TIMESTAMP:
              udtValue.setTimestamp(key, (Date) value);
              break;

            case TIMEUUID:
            case UUID:
              udtValue.setUUID(key, (UUID) value);
              break;

            case TEXT:
            case VARCHAR:
              udtValue.setString(key, (String) value);
              break;
            default:
              throw new ParseException("UDT field type '" +
                  userType.getFieldType(key).getName().toString() + "'not supported yet", 0);
          }
        }

        i++;
      }
    }
    catch (Exception e) {
      System.err.println("Trouble parsing : " + e.getMessage());
      e.printStackTrace();
      return null;
    }
    return udtValue;
  }

  @SuppressWarnings("unchecked")
  public String format(Object o) {
    Map<Object,Object> map = (Map<Object,Object>)o;
    Iterator<Map.Entry<Object,Object> > iter = map.entrySet().iterator();
    Map.Entry<Object,Object> me;
    StringBuilder sb = new StringBuilder();
    sb.append(collectionBegin);
    int i = 0;
    if (iter.hasNext()) {
      me = iter.next();
      sb.append(fieldNameParser.format(me.getKey()));
      sb.append(mapDelim);
      sb.append(fieldValueParsers.get(i).format(me.getValue()));
    }
    while (iter.hasNext()) {
      i++;
      sb.append(collectionDelim);
      me = iter.next();
      sb.append(fieldNameParser.format(me.getKey()));
      sb.append(mapDelim);
      sb.append(fieldValueParsers.get(i).format(me.getValue()));
    }
    sb.append(collectionEnd);

    return quote(sb.toString());
  }
}
