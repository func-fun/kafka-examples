import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Command } from "commander";
import { Kafka, CompressionTypes, KafkaConfig, SASLOptions } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { AvroProducer } from "@func-fun/kafka-avro-lib/avroProducer";
import type { TopicAvroSettings } from "@func-fun/kafka-avro-lib/types";

function loadJsonFile(filePath: string) {
  const raw = readFileSync(resolve(filePath), "utf-8");
  // Strip single-line comments (// ...) to support .jsonc
  const stripped = raw.replace(/^\s*\/\/.*$/gm, "");
  return JSON.parse(stripped);
}

const program = new Command();

program
  .name("produce")
  .description("Produce Avro messages to a Kafka topic")
  .option("-c, --config <path>", "Path to config file", "config.example.jsonc")
  .option("-m, --messages <path>", "Path to messages JSON file", "messages.example.json")
  .action(async (opts) => {
    const config = loadJsonFile(opts.config);
    const messages: { topic: string; key: string; payload: Record<string, unknown> }[] =
      loadJsonFile(opts.messages);

    const kafkaConfig: KafkaConfig = {
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
    };

    if (config.kafka.sasl) {
      kafkaConfig.ssl = true;
      kafkaConfig.sasl = {
        mechanism: config.kafka.sasl.mechanism,
        username: config.kafka.sasl.username,
        password: config.kafka.sasl.password,
      } as SASLOptions;
    }

    if (config.kafka.ssl) {
      kafkaConfig.ssl = {
        ca: readFileSync(resolve(config.kafka.ssl.ca)),
        cert: readFileSync(resolve(config.kafka.ssl.cert)),
        key: readFileSync(resolve(config.kafka.ssl.key)),
      };
    }

    const kafka = new Kafka(kafkaConfig);
    const schemaRegistry = new SchemaRegistry(config.schemaRegistry);

    for (const msg of messages) {
      const topicConfig = config.topics.find(
        (t: { topicName: string }) => t.topicName === msg.topic
      );
      if (!topicConfig) {
        console.error(`Error: topic "${msg.topic}" not found in config, skipping`);
        continue;
      }

      const topicAvroSettings: TopicAvroSettings[] = [
        {
          ...topicConfig,
          compression: CompressionTypes.GZIP,
        },
      ];

      const producer = new AvroProducer(kafka, schemaRegistry, topicAvroSettings);

      try {
        const result = await producer.produceSingleMessage(
          msg.key,
          msg.payload,
          msg.topic,
        );
        console.log(`Message "${msg.key}" produced successfully:`, result);
      } catch (err) {
        console.error(`Failed to produce message "${msg.key}":`, (err as Error).message);
      }
    }
  });

program.parse();
