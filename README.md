
# CQL data adapters for Magic and Hyperlambda

This project provides Magic and Hyperlambda with CQL data adapters, allowing you to perform CRUD operations towards
for instance Cassandra or ScyllaDB. In addition the project provides alternative fil/folder storage for Magic,
storing files and folders in a CQL based database, such as Cassandra and ScyllaDB.

## Configuration

The primary configuration for the project to apply for your _"appsettings.json"_ file can be found below.

```json
{
  "magic": {
    "cql": {
      "host": "127.0.0.1"
    }
  }
}
```

The above configures the adapter to use `127.0.0.1` as the host for your contact point. To configure the adapter
to store files and folders inside of its CQL based database, you can alternatively add something such as follows
to your _"appsettings.json"_ file.

```json
{
  "magic": {
    "io": {
      "file-service": "magic.data.cql.io.CqlFileService",
      "folder-service": "magic.data.cql.io.CqlFolderService",
      "stream-service": "",
      "root-resolver": ""
    }
  }
}
```

## Database schema

To create the database use the following CQL.

```sql
create keyspace if not exists magic with replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };
use magic;
create table files(cloudlet text, folder text, filename text, content text, primary key(cloudlet, folder, filename));
```
