/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.mongodb.templates;

import static com.google.cloud.teleport.v2.utils.GCSUtils.getGcsFileAsString;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.PubSubOptions;
import com.google.cloud.teleport.v2.mongodb.templates.MongoDbCdcToBigQuery.Options;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MongoDbCdcToBigQuery} pipeline is a streaming pipeline which reads data pushed to
 * PubSub from MongoDB Changestream and outputs the resulting records to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/mongodb-to-googlecloud/README_MongoDB_to_BigQuery_CDC.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "MongoDB_to_BigQuery_CDC",
    category = TemplateCategory.STREAMING,
    displayName = "MongoDB (CDC) to BigQuery",
    description =
        "The MongoDB CDC (Change Data Capture) to BigQuery template is a streaming pipeline that works together with MongoDB change streams. "
            + "The pipeline reads the JSON records pushed to Pub/Sub via a MongoDB change stream and writes them to BigQuery as specified by the <code>userOption</code> parameter.",
    optionsClass = Options.class,
    flexContainerName = "mongodb-to-bigquery-cdc",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The target BigQuery dataset must exist.",
      "The source MongoDB instance must be accessible from the Dataflow worker machines.",
      "The change stream pushing changes from MongoDB to Pub/Sub should be running."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class MongoDbCdcToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbCdcToBigQuery.class);

  /** Options interface. */
  public interface Options
      extends PipelineOptions,
          PubSubOptions,
          BigQueryWriteOptions,
          JavascriptDocumentTransformerOptions,
          BigQueryStorageApiStreamingOptions {

    // Hide the UseStorageWriteApiAtLeastOnce in the UI, because it will automatically be turned
    // on when pipeline is running on ALO mode and using the Storage Write API
    @TemplateParameter.Boolean(
        order = 1,
        optional = true,
        parentName = "useStorageWriteApi",
        parentTriggerValues = {"true"},
        description = "Use at at-least-once semantics in BigQuery Storage Write API",
        helpText =
            "When using the Storage Write API, specifies the write semantics. To"
                + " use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-"
                + " once semantics, set the parameter to `false`. This parameter applies only when"
                + " `useStorageWriteApi` is `true`. The default value is `false`.",
        hiddenUi = true)
    @Default.Boolean(false)
    @Override
    Boolean getUseStorageWriteApiAtLeastOnce();

    void setUseStorageWriteApiAtLeastOnce(Boolean value);
  }

  /** class ParseAsDocumentsFn. */
  private static class ParseAsDocumentsFn extends DoFn<String, Document> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args)
      throws ScriptException, IOException, NoSuchMethodException {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    run(options);
  }

  /** Pipeline to read data from PubSub and write to MongoDB. */
  public static boolean run(Options options)
      throws ScriptException, IOException, NoSuchMethodException {
    options.setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();
    String inputSubscription = options.getInputSubscription();

    TableSchema bigquerySchema;

    if (options.getBigQuerySchemaPath() == null || options.getBigQuerySchemaPath().isEmpty()) {
      throw new RuntimeException("Required " + options.getBigQuerySchemaPath() + " is not set or missing.");
    }

    bigquerySchema = loadBigQuerySchemaFromGcs(options);

    LOG.info(bigquerySchema.toPrettyString());

    // Define a function to generate RowMutationInformation from a TableRow
    SerializableFunction<TableRow, RowMutationInformation> mutationFnForUpsert = tableRow -> {
        String sequenceNumber = Long.toHexString((Long) tableRow.get("_ts"));
        
        return RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, sequenceNumber);
    };

    pipeline
        .apply("Read PubSub Messages", PubsubIO.readStrings().fromSubscription(inputSubscription))
        .apply(
            "RTransform string to document",
            ParDo.of(
                new DoFn<String, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = Document.parse(c.element());
                    c.output(document);
                  }
                }))
        .apply(
            "Read and transform data",
            ParDo.of(
                new DoFn<Document, TableRow>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    TableRow row = MongoDbUtils.getTableSchema(document, userOption);
                    c.output(row);
                  }
                }))
        .apply(
            BigQueryIO.writeTableRows()
                .to(options.getOutputTableSpec())
                .withSchema(bigquerySchema)
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withPrimaryKey(ImmutableList.of(options.getOutputTablePrimaryKey()))
                .withRowMutationInformationFn(mutationFnForUpsert)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .ignoreUnknownValues()
              );
    pipeline.run();
    return true;
  }

  private static TableSchema loadBigQuerySchemaFromGcs(Options options) throws IOException {
    // initialize FileSystem to read from GCS
    FileSystems.setDefaultPipelineOptions(options);
    
    String jsonSchema = getGcsFileAsString(options.getBigQuerySchemaPath());
    GsonFactory gf = new GsonFactory();
    return gf.fromString(jsonSchema, TableSchema.class);
  }
}
