# kafka-examples

CLI tools for producing Avro messages to Kafka using [@func-fun/kafka-avro-lib](https://github.com/func-fun/kafka-lib).

## Prerequisites

- Node.js >= 16
- Yarn

## Setup

```bash
yarn install
```

## Configuration

Copy and edit the example config file:

```bash
cp config.example.jsonc config.jsonc
```

The config file contains:

- **kafka** — broker list, client ID, and authentication (SASL or SSL)
- **schemaRegistry** — host and optional auth credentials
- **topics** — Avro topic mappings (subject names, schema versions)

### Authentication

The example config uses SASL (`scram-sha-512`). To use SSL client certificates instead, replace the `sasl` block with:

```jsonc
"ssl": {
  "ca": "./certs/ca.pem",
  "cert": "./certs/client.pem",
  "key": "./certs/client-key.pem"
}
```

## Messages

Copy and edit the example messages file:

```bash
cp messages.example.json messages.json
```

The messages file is a JSON array where each entry has:

| Field     | Description                        |
|-----------|------------------------------------|
| `topic`   | Topic name (must match config)     |
| `key`     | Message key                        |
| `payload` | Message payload object             |

## Produce

```bash
# Using defaults (config.example.jsonc + messages.example.json)
yarn produce

# Custom config and messages
yarn produce -c config.jsonc -m messages.json
```

### Options

| Flag                  | Description                | Default                  |
|-----------------------|----------------------------|--------------------------|
| `-c, --config <path>` | Path to config file       | `config.example.jsonc`   |
| `-m, --messages <path>`| Path to messages JSON file| `messages.example.json`  |
| `-h, --help`          | Display help               |                          |