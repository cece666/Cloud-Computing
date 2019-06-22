# Practical Course Cloud Databases - MS4

## Protocol

For sending messages, 1 byte each is reserved for encoding the message length, status, key length and value length.
Supposing key length and value length are n and m resp, then n and m bytes are used for encoding the key and value.

```plaintext
<messageLength: 1 byte>
<status: 1 byte>
<keyLength: 1 byte>
<valueLength: 1 byte>
<key: n bytes>
<value: m bytes>
```

## Additonal Statuses

Two more statuses were added.

**INFO**, for informing the client of something

**FAIL**, in case of error

## Commands for interacting with the server

```plaintext
-connect <addr> <port>
-disconnect
-put <key> <value>
-put <key>
-get <key>
-logLevel <level>
-help
```

## Database implementation

Each pair is started with 1 byte of key length and 1 byte of value length, follow by the actual key and value. All the pairs are written right next to each other.
Everytime the database needs to read or modify the data, it loads the whole data into memory, read the neccesary data or modify and write the whole data back to disk.

## Run instruction

### Make sure that you don't need to enter password for the servers that you are initialzing

1. create a softlink `~/cloud_databases` pointing to the project.
2. run the ECS jar file and follow the `help` string.

## Performance measurement

1. Make sure there's a server running on port 50000.
2. run `java -jar ~/cloud_database/ms3-client.jar <nClients>` to start performance measurement.

## Ambiguous behavior

* Deleting a none existing pair: If a user make a `put <key>` command and the key does not exist in the database, the application considers operation is executed successfully.

* When using the `put` and `get` commands, all the spaces before keys and values are discarded, hence all keys and values cannot begin with spaces. This also means that both keys and values cannot be blank.