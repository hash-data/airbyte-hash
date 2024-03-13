/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.bigquery;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.fasterxml.jackson.databind.JsonNode;
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
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.airbyte.db.SqlDatabase;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class BigQueryDatabase extends SqlDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDatabase.class);
  private static final String AGENT_TEMPLATE = "%s (GPN: Airbyte; staging)";
  private  String datasetName ="temp";
  private final BigQuery bigQuery;
  private final BigQuerySourceOperations sourceOperations;
  private int readThreads;
  private final BaseBigQueryReadClient baseBigQueryReadClient;

  public BigQueryDatabase(final String projectId, final String jsonCreds, final Integer threads) {
    this(projectId, jsonCreds,threads, new BigQuerySourceOperations());
  }

  public BigQueryDatabase(final String projectId, final String jsonCreds, final Integer threads, final BigQuerySourceOperations sourceOperations) {
    try {
      System.out.println("threads set to : "+threads);
      this.readThreads = threads;
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
      // bigquery storage client
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
  @Override
  public void execute(final String sql) throws SQLException {
    final String jobId = UUID.randomUUID().toString();
    final ImmutablePair<Job, String> result = executeQuery(bigQuery,jobId, getQueryConfig(sql,jobId,Collections.emptyList()));
    if (result.getLeft() == null) {
      throw new SQLException("BigQuery request is failed with error: " + result.getRight() + ". SQL: " + sql);
    }
    // add expiration time of one day to the table created for query results
    bigQuery.update(bigQuery.getTable(datasetName, jobId).toBuilder().setExpirationTime(TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS) + System.currentTimeMillis()).build());

    LOGGER.info("BigQuery successfully finished execution SQL: " + sql);
  }
  public Stream<JsonNode> processRowsFromStream(ReadStream stream, AvroReader reader) throws IOException {
    String streamName = stream.getName();
    ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();
    ServerStream<ReadRowsResponse> streamResponse = baseBigQueryReadClient.readRowsCallable().call(readRowsRequest);
    List<ByteString> byteArrayResponse = new ArrayList<>();
    for (ReadRowsResponse response : streamResponse) {
      byteArrayResponse.add(response.getAvroRows().getSerializedBinaryRows());
    }

    List<JsonNode> jsonNodes = new ArrayList<>();
    for (ByteString byteString : byteArrayResponse) {
      jsonNodes.addAll(reader.processRows(byteString).toList());
    }
    return jsonNodes.stream();
  }
  public CustomIterator readDataFromTable(String projectID, String tablePath) throws InterruptedException, ExecutionException {
    // table path e.g. : "projects/oauth-361809/datasets/testfasterbq/tables/test1"
    ReadSession readSession = ReadSession.newBuilder()
            .setTable(tablePath).setDataFormat(DataFormat.AVRO)
            .build();
    ProjectName parent = ProjectName.of(projectID);
    CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder().setParent(parent.toString()).setReadSession(readSession);
    ReadSession session = baseBigQueryReadClient.createReadSession(builder.build());
    ExecutorService executorService = Executors.newFixedThreadPool(readThreads);
    AvroReader reader = new AvroReader(session.getAvroSchema());
    System.out.println("number of streams received here : "+session.getStreamsCount());
    List<Future<Stream<JsonNode>>> futures = new ArrayList<>();
    for (ReadStream stream : session.getStreamsList()) {
      Callable<Stream<JsonNode>> task = () -> processRowsFromStream(stream, reader);
      Future<Stream<JsonNode>> future = executorService.submit(task);
      futures.add(future);
    }
    CustomIterator iterator = new CustomIterator(futures);
    executorService.shutdown();
    return iterator;
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

  public Stream<JsonNode> query(final String sql, final List<QueryParameterValue> params) throws Exception {
    final String jobId = UUID.randomUUID().toString();
    Instant t1 = Instant.now();
    final ImmutablePair<Job, String> result = executeQuery(bigQuery,jobId, getQueryConfig(sql,jobId,params));
    System.out.println("time taken to query on Bigquery: "+java.time.Duration.between(t1,Instant.now()).toSeconds());
    if (result.getLeft() != null) {
      Table table = bigQuery.getTable(datasetName, jobId);
      // add expiration time of one week to the table created for query results
      bigQuery.update(table.toBuilder().setExpirationTime(TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS) + System.currentTimeMillis()).build());
      String projectName = table.getTableId().getProject();
      String tableName = table.getTableId().getTable();
     return Streams.stream(readDataFromTable(projectName,"projects/"+ projectName+ "/datasets/"+ datasetName+"/tables/"+tableName));
    } else
      throw new Exception(
          "Failed to execute query " + sql + (params != null && !params.isEmpty() ? " with params " + params : "") + ". Error: " + result.getRight());
  }

  public QueryJobConfiguration getQueryConfig(final String sql, final String jobId, final List<QueryParameterValue> params) {
    return QueryJobConfiguration
        .newBuilder(sql)
        .setUseLegacySql(false)
        .setPositionalParameters(params)
        .setDestinationTable(TableId.of(datasetName,jobId))
        .setAllowLargeResults(true)
        .build();
  }

  public ImmutablePair<Job, String> executeQuery(final BigQuery bigquery,final String jobId, final QueryJobConfiguration queryConfig) {
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
