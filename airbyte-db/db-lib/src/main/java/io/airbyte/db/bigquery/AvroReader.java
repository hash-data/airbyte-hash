package io.airbyte.db.bigquery;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class AvroReader {
  private ObjectMapper objectMapper = new ObjectMapper();
  private final GenericDatumReader<GenericRecord> datumReader;
  public AvroReader(AvroSchema arrowSchema) {
    Schema schema = new Schema.Parser().parse(arrowSchema.getSchema());
    this.datumReader = new GenericDatumReader<>(schema);
  }
  public Stream<JsonNode> processRows(ByteString avroRows) throws IOException {
    List<JsonNode> jsonNodes = new ArrayList<>();
    try (InputStream inputStream = new ByteArrayInputStream(avroRows.toByteArray())) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      while (!decoder.isEnd()) {
        GenericRecord item = datumReader.read(null, decoder);
        JsonNode jsonNode = convertGenericRecordToJsonNode(item);
        jsonNodes.add(jsonNode);
      }
    }
    return jsonNodes.stream();
  }

  private JsonNode convertGenericRecordToJsonNode(GenericRecord record) {
    // Assuming the Avro schema fields are fixed and known
    ObjectNode objectNode = objectMapper.createObjectNode();
    // Iterate over the fields in the schema and add them to the JSON object
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.name();
      Schema.Type fieldType = field.schema().getType();
      // Handle different field types accordingly
      switch (fieldType) {
        case ARRAY:
          // Handle array fields
          ArrayNode arrayNode = convertGenericArrayToArrayNode((GenericArray) record.get(fieldName));
          objectNode.set(fieldName, arrayNode);
          break;
        default:
          // For other types, simply add the field to the JSON object
          objectNode.put(fieldName, record.get(fieldName).toString());
          break;
      }
    }
    return objectNode;
  }

  private ArrayNode convertGenericArrayToArrayNode(GenericArray array) {
    ArrayNode arrayNode = objectMapper.createArrayNode();
    for (Object element : array) {
      // Convert each element in the array to a JSON node
      arrayNode.add(element.toString());
    }
    return arrayNode;
  }
}
