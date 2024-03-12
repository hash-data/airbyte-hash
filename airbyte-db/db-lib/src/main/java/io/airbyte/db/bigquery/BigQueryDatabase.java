/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.bigquery;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.airbyte.db.SqlDatabase;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class BigQueryDatabase extends SqlDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDatabase.class);
  private static final String AGENT_TEMPLATE = "%s (GPN: Airbyte; staging)";
  private String datasetName = "temp";
  private final BigQuery bigQuery;
  private final BaseBigQueryReadClient baseBigQueryReadClient;
  private final BigQuerySourceOperations sourceOperations;

  public BigQueryDatabase(final String projectId, final String jsonCreds) {
    this(projectId, jsonCreds, new BigQuerySourceOperations());
  }

  public BigQueryDatabase(final String projectId, final String jsonCreds, final BigQuerySourceOperations sourceOperations) {
    try {
      this.sourceOperations = sourceOperations;
      final BigQueryOptions.Builder bigQueryBuilder = BigQueryOptions.newBuilder();
      ServiceAccountCredentials credentials = null;
      if (jsonCreds != null && !jsonCreds.isEmpty()) {
        credentials = ServiceAccountCredentials
            .fromStream(new ByteArrayInputStream(jsonCreds.getBytes(Charsets.UTF_8)));
      }
      bigQuery = bigQueryBuilder
          .setProjectId(projectId)
          .setCredentials(!isNull(credentials) ? credentials : ServiceAccountCredentials.getApplicationDefault())
          .setHeaderProvider(() -> ImmutableMap.of("user-agent", getUserAgentHeader(getConnectorVersion())))
          .setRetrySettings(RetrySettings
              .newBuilder()
              .setMaxAttempts(10)
              .setRetryDelayMultiplier(1.5)
              .setTotalTimeout(Duration.ofMinutes(60))
              .build())
          .build()
          .getService();
      BaseBigQueryReadSettings baseBigQueryReadSettings =
          BaseBigQueryReadSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
      baseBigQueryReadClient = BaseBigQueryReadClient.create(baseBigQueryReadSettings);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getUserAgentHeader(final String connectorVersion) {
    return String.format(AGENT_TEMPLATE, connectorVersion);
  }

  private String getConnectorVersion() {
    return Optional.ofNullable(System.getenv("WORKER_CONNECTOR_IMAGE"))
        .orElse(EMPTY)
        .replace("airbyte/", EMPTY).replace(":", "/");
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  @Override
  public void execute(final String sql) throws SQLException {
    final String jobId = UUID.randomUUID().toString();
    final ImmutablePair<Job, String> result = executeQuery(bigQuery, jobId, getQueryConfig(sql, jobId, Collections.emptyList()));
    if (result.getLeft() == null) {
      throw new SQLException("BigQuery request is failed with error: " + result.getRight() + ". SQL: " + sql);
    }
    // add expiration time of one day to the table created for query results
    bigQuery.update(bigQuery.getTable(datasetName, jobId).toBuilder()
        .setExpirationTime(TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS) + System.currentTimeMillis()).build());

    LOGGER.info("BigQuery successfully finished execution SQL: " + sql);
  }

  public Stream<JsonNode> query(final String sql) throws Exception {
    return query(sql, Collections.emptyList());
  }

  public Stream<JsonNode> query(final String sql, final QueryParameterValue... params) throws Exception {
    return query(sql, (params == null ? Collections.emptyList() : Arrays.asList(params)));
  }

  @Override
  public Stream<JsonNode> unsafeQuery(final String sql, final String... params) throws Exception {
    final List<QueryParameterValue> parameterValueList;
    if (params == null)
      parameterValueList = Collections.emptyList();
    else
      parameterValueList = Arrays.stream(params).map(param -> QueryParameterValue.newBuilder().setValue(param).setType(
          StandardSQLTypeName.STRING).build()).collect(Collectors.toList());

    return query(sql, parameterValueList);
  }

  public class AvroReader {

    private final GenericDatumReader<GenericRecord> datumReader;

    public AvroReader(AvroSchema arrowSchema) {
      Schema schema = new Schema.Parser().parse(arrowSchema.getSchema());
      this.datumReader = new GenericDatumReader<>(schema);
    }

    public void processRows(ByteString avroRows) throws IOException {
      try (InputStream inputStream = new ByteArrayInputStream(avroRows.toByteArray())) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        while (!decoder.isEnd()) {
          GenericRecord item = datumReader.read(null, decoder);
          // System.out.println(item);
        }
      }
    }

  }

  public Stream<JsonNode> query(final String sql, final List<QueryParameterValue> params) throws Exception {
//     final String jobId = UUID.randomUUID().toString();
//     final ImmutablePair<Job, String> result = executeQuery(bigQuery,jobId,
//     getQueryConfig(sql,jobId,params));
//
//     if (result.getLeft() != null) {
//     final FieldList fieldList = result.getLeft().getQueryResults().getSchema().getFields();
     // add expiration time of one week to the table created for query results
//     bigQuery.update(bigQuery.getTable(datasetName,
//     jobId).toBuilder().setExpirationTime(TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS) +
    // System.currentTimeMillis()).build());
//    ProjectName parent = ProjectName.of("oauth-361809"); // Use actual project ID
//    ReadSession readSession = ReadSession.newBuilder()
//        .setTable("projects/oauth-361809/datasets/testfasterbq/tables/random_data2").setDataFormat(DataFormat.AVRO)
//        .build();
    // ReadSession response = baseBigQueryReadClient.createReadSession(parent.toString(), readSession,
    // 940837515);
    CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder().setParent(parent.toString()).setReadSession(readSession);
    ReadSession session = baseBigQueryReadClient.createReadSession(builder.build());
    final Instant start = Instant.now();
    System.out.println("stream count: " + session.getStreamsCount());
    int threadId = 0;
    ExecutorService executor = Executors.newFixedThreadPool(300);
    for (ReadStream stream : session.getStreamsList()) {
      threadId++;
      int finalThreadId = threadId;
      Callable<String> callableTask = () -> {
        if(finalThreadId == 1) {
          System.out.println("started callable task");
        }
        long rowCount = 0;
        String streamName = stream.getName();
        ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();
        Instant t0 = Instant.now();
        ServerStream<ReadRowsResponse> streamResponse = baseBigQueryReadClient.readRowsCallable().call(readRowsRequest);

        List<ByteString> byteArrayResponse = new ArrayList<>();
        for (ReadRowsResponse response : streamResponse) {
          rowCount+=response.getRowCount();
          byteArrayResponse.add(response.getAvroRows().getSerializedBinaryRows());
        }
        System.out.println("Time to get response rows : " + java.time.Duration.between(t0, Instant.now()).getSeconds()+" : sessionID: "+finalThreadId);
        Instant t1 = Instant.now();
        for (ByteString byteString : byteArrayResponse) {
          new AvroReader(session.getAvroSchema()).processRows(byteString);

        }
        System.out.println("Time to parse rows : " + java.time.Duration.between(t1, Instant.now()).getSeconds());
        System.out.println("SessionID " + finalThreadId + " RowsReceived : " + rowCount);
        return "Done" + finalThreadId;
      };
      executor.submit(callableTask);
    }

    // AtomicInteger rowCount = new AtomicInteger(0);
    // for (ReadStream stream : session.getStreamsList()) {
    // executor.submit(() -> {
    // String streamName = stream.getName();
    // ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();
    // ServerStream<ReadRowsResponse> streamResponse =
    // baseBigQueryReadClient.readRowsCallable().call(readRowsRequest);
    // System.out.println("finished till here");
    // for (ReadRowsResponse response : streamResponse) {
    // rowCount.addAndGet((int)(response.getRowCount()));
    // try {
    // new
    // AvroReader(session.getAvroSchema()).processRows(response.getAvroRows().getSerializedBinaryRows());
    // } catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    // }
    // });
    // }

    // Wait for all threads to finish
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    System.out.println("Time to parse all data : " + java.time.Duration.between(start, Instant.now()).getSeconds());

    // System.out.println("Total rows processed: " + rowCount.get());
    // for(ReadStream stream1 : session.getStreamsList()) {
    // String streamName = stream1.getName();
    // ReadRowsRequest readRowsRequest =
    // ReadRowsRequest.newBuilder().setReadStream(streamName).build();
    // ServerStream<ReadRowsResponse> stream =
    // baseBigQueryReadClient.readRowsCallable().call(readRowsRequest);
    // System.out.println("finished till here");
    // for (ReadRowsResponse response : stream) {
    // rowCount.incrementAndGet();
    // new
    // AvroReader(session.getAvroSchema()).processRows(response.getAvroRows().getSerializedBinaryRows());
    // }
    // }
    // final Instant end = Instant.now();
    // final java.time.Duration durat = java.time.Duration.between(start,end);
    // System.out.println("Total records iterated: " + rowCount.get() +" : in time millis:
    // "+durat.toMillis());
    // System.out.println("here read session is received: "+ response);
//     return Streams.stream(result.getLeft().getQueryResults().iterateAll())
//     .map(fieldValues -> sourceOperations.rowToJson(new BigQueryResultSet(fieldValues, fieldList)));
    return null;
    // } else
    // throw new Exception(
    // "Failed to execute query " + sql + (params != null && !params.isEmpty() ? " with params " +
    // params : "") + ". Error: " + result.getRight());
  }

  public QueryJobConfiguration getQueryConfig(final String sql, final String jobId, final List<QueryParameterValue> params) {
    return QueryJobConfiguration
        .newBuilder(sql)
        .setUseLegacySql(false)
        .setPositionalParameters(params)
        .setDestinationTable(TableId.of(datasetName, jobId))
        .setAllowLargeResults(true)
        .build();
  }

  public ImmutablePair<Job, String> executeQuery(final BigQuery bigquery, final String jobId, final QueryJobConfiguration queryConfig) {
    final Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(JobId.of(jobId)).build());
    return executeQuery(queryJob);
  }

  /**
   * Returns full information about all tables from entire project
   *
   * @param projectId BigQuery project id
   * @return List of BigQuery tables
   */
  public List<Table> getProjectTables(final String projectId) {
    final List<Table> tableList = new ArrayList<>();
    bigQuery.listDatasets(projectId)
        .iterateAll()
        .forEach(dataset -> bigQuery.listTables(dataset.getDatasetId())
            .iterateAll()
            .forEach(table -> tableList.add(bigQuery.getTable(table.getTableId()))));
    return tableList;
  }

  /**
   * Returns full information about all tables from specific Dataset
   *
   * @param datasetId BigQuery dataset id
   * @return List of BigQuery tables
   */
  public List<Table> getDatasetTables(final String datasetId) {
    final List<Table> tableList = new ArrayList<>();
    bigQuery.listTables(datasetId)
        .iterateAll()
        .forEach(table -> tableList.add(bigQuery.getTable(table.getTableId())));
    return tableList;
  }

  public BigQuery getBigQuery() {
    return bigQuery;
  }

  public void cleanDataSet(final String dataSetId) {
    // allows deletion of a dataset that has contents
    final BigQuery.DatasetDeleteOption option = BigQuery.DatasetDeleteOption.deleteContents();

    final boolean success = bigQuery.delete(dataSetId, option);
    if (success) {
      LOGGER.info("BQ Dataset " + dataSetId + " deleted...");
    } else {
      LOGGER.info("BQ Dataset cleanup for " + dataSetId + " failed!");
    }
  }

  private ImmutablePair<Job, String> executeQuery(final Job queryJob) {
    final Job completedJob = waitForQuery(queryJob);
    if (completedJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (completedJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      return ImmutablePair.of(null, (completedJob.getStatus().getError().toString()));
    }

    return ImmutablePair.of(completedJob, null);
  }

  private Job waitForQuery(final Job queryJob) {
    try {
      return queryJob.waitFor();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

}
