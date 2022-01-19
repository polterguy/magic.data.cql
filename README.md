
# ScyllaDB data adapters for Magic

ScyllaDB data adapters for Magic

## Database schema

To create the database use the following CQL.

```sql
create keyspace if not exists magic with replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };
use magic;
create table files(client text, cloudlet text, filename text, content text, primary key(client, cloudlet, filename));
```
