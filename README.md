
# CQL data adapters for Magic and Hyperlambda

This project provides Magic and Hyperlambda with CQL data adapters, allowing you to perform CRUD operations towards
for instance Cassandra or ScyllaDB. In addition the project provides alternative file/folder storage for Magic,
storing files and folders in a CQL based database, such as Cassandra or ScyllaDB.

## Slots

The project contains the following slots.

* __[cql.connect]__ - Creates a session towards a CQL cluster
* __[cql.execute]__ - Executes some CQL statement towards a previously opened session and returns the result to caller

The basic idea of the slots are to allow for this such as follows.

```
cql.connect:[generic|magic]
   cql.select:"select * from files where cloudlet = 'foo/bar' and folder = '/etc/' and filename like 'howdy%'"
```

Where the `generic` parts above is a reference to a cluster, you'll have to configure in your _"appsettings.json"_,
while the `magic` parts above is a keyspace within that cluster. In such a regard the slots resembles the generic
RDBMS slots in usage, except of course it open a connection towards a NoSQL database such as Cassandra or ScyllaDB,
and returns the result of executing your SQL towards a keyspace within that cluster.

## Alternative file system services

The adapter also contains alternative file system services, implementing `IFileService`, `IFolderService`, and
`IStreamService`, allowing you to use it interchangeable as a _"virtual file system"_ for cases where you want
to have 100% stateless magic instances, which is important if you're using Magic in a Kubernetes cluster or
something similar, load balancing invocations, virtually resolving towards your virtual file system.
If you take this path you'll have to configure your _"appsettings.json"_ file such as illustrated further
down in this document.

## Configuration

The primary configuration for the project to apply for your _"appsettings.json"_ file can be found below.

```json
{
  "magic": {
    "cql": {
      "generic": {
        "host": "127.0.0.1"
      }
    }
  }
}
```

The above configures the adapter to use `127.0.0.1` as the host for your contact point or cluster. To configure
the adapter to store files and folders inside of its CQL based database, you can alternatively add something such
as follows to your _"appsettings.json"_ file.

```json
{
  "magic": {
    "io": {
      "file-service": "magic.data.cql.io.CqlFileService",
      "folder-service": "magic.data.cql.io.CqlFolderService",
      "stream-service": "magic.data.cql.io.CqlStreamService"
    }
  }
}
```

If you want to use a CQL based virtual file system, you'll have to create a keyspace called _"magic"_
within your _"generic"_ cluster connection, with a table named _"files"_. Below is an example of how
you could achieve this using CQL.

```sql
create keyspace if not exists magic with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
use magic;
create table if not exists files(cloudlet text, folder text, filename text, content text, primary key(cloudlet, folder, filename));
```

## Adding existing files into keyspace

The following Hyperlambda will insert all your existing files and folders into your cluster keyspace, allowing you to
play around with an existing CQL file system implementation. Notice, you'll have to change the **[.root]** value to resemble
the absolute root folder for your Magic backend.

```
/*
 * Inserts all dynamic files and folders into the magic CQL database.
 */
cql.connect:magic

   /*
    * The root folder where your Magic backend is running.
    */
   .root:"/Users/thomashansen/Documents/projects/magic/magic/backend/"

   /*
    * Inserting root folder.
    */
   cql.execute:"insert into files (cloudlet, folder, filename, content) values ('/Users/thomashansen/Documents/projects/magic/magic/backend/', '/files/', '', '')"

   /*
    * Inserting appsettings.json
    */
   config.load
   cql.execute:"insert into files (cloudlet, folder, filename, content) values (:cloudlet, '/config/', 'appsettings.json', :config)"
      cloudlet:x:@.root
      config:x:@config.load

   /*
    * Inserting folders.
    */
   signal:magic.io.folder.list-recursively
      .:/
   for-each:x:-/*

      strings.concat
         .:/files
         get-value:x:@.dp/#
      
      cql.execute:"insert into files (cloudlet, folder, filename, content) values (:cloudlet, :folder, '', '')"
         cloudlet:x:@.root
         folder:x:@strings.concat

   /*
    * Inserting files.
    */
   signal:magic.io.file.load-recursively
      .:/
   for-each:x:-/*
   
      strings.split:x:@.dp/#
         .:/
      unwrap:x:+
      .filename:x:@strings.split/0/-
      remove-nodes:x:@strings.split/0/-
      strings.join:x:@strings.split/*
         .:/
      strings.concat
         .:/files/
         get-value:x:@strings.join
         .:/
      strings.replace:x:-
         .://
         .:/
      cql.execute:"insert into files (cloudlet, folder, filename, content) values (:cloudlet, :folder, :filename, :content)"
         cloudlet:x:@.root
         folder:x:@strings.replace
         filename:x:@.filename
         content:x:@.dp/#/*

remove-nodes:x:../**/signal/*
```
