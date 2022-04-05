/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using magic.node.contracts;
using magic.node.extensions;
using magic.data.cql.helpers;
using magic.lambda.logging.contracts;

namespace magic.data.cql.logging
{
    /// <inheritdoc/>
    public class Logger : ILogger, ILogQuery
    {
        readonly IRootResolver _rootResolver;
        readonly IConfiguration _configuration;
        readonly LogSettings _settings;

        /// <summary>
        /// Constructs a new instance of the default ILogger implementation.
        /// </summary>
        /// <param name="rootResolver">Needed to figure out tenant and cloudlet IDs.</param>
        /// <param name="configuration">Configuration object.</param>
        /// <param name="settings">Configuration object.</param>
        public Logger(
            IRootResolver rootResolver,
            IConfiguration configuration,
            LogSettings settings)
        {
            _rootResolver = rootResolver;
            _configuration = configuration;
            _settings = settings;
        }

        #region [ -- Interface implementations -- ]

        #region [ -- ILogger interface implementation -- ]

        /// <inheritdoc/>
        public Task DebugAsync(string content)
        {
            return InsertLogEntryAsync("debug", content, null, null);
        }

        /// <inheritdoc/>
        public Task DebugAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync("debug", content, meta, null);
        }

        /// <inheritdoc/>
        public Task InfoAsync(string content)
        {
            return InsertLogEntryAsync("info", content, null, null);
        }

        /// <inheritdoc/>
        public Task InfoAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync("info", content, meta, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content)
        {
            return InsertLogEntryAsync("error", content, null, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync("error", content, meta, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, string stackTrace)
        {
            return InsertLogEntryAsync("error", content, null, stackTrace);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, Dictionary<string, string> meta, string stackTrace)
        {
            return InsertLogEntryAsync("error", content, meta, stackTrace);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content)
        {
            return InsertLogEntryAsync("fatal", content, null, null);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync("fatal", content, meta, null);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, string stackTrace)
        {
            return InsertLogEntryAsync("fatal", content, null, stackTrace);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, Dictionary<string, string> meta, string stackTrace)
        {
            return InsertLogEntryAsync("fatal", content, meta, stackTrace);
        }

        #endregion

        #region [ -- ILogQuery interface implementation -- ]

        /// <inheritdoc/>
        public async Task<IEnumerable<LogItem>> QueryAsync(int max, object fromId, string content = null)
        {
            // Sanity checking invocation.
            if (content != null)
                throw new HyperlambdaException("The NoSQL data adapter doesn't support content filtering for log items");

            // Creating a cluster session using magic_log keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                // Creating builder to hold our CQL.
                var builder = new StringBuilder(
                    "select created as id, toTimestamp(created) as created, type, content, exception, meta from log where tenant = ? and cloudlet = ?");

                // Creating arguments to hold our arguments.
                List<object> args = new List<object>();
                var ids = Utilities.Resolve(_rootResolver);
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);

                // Checking if we're paging from a specific log item.
                if (fromId != null)
                {
                    builder.Append($" and created < ?");
                    args.Add(Guid.Parse(fromId.ToString()));
                }

                // Appending tail.
                builder.Append($" order by created desc limit {max}");

                // Executing CQL and returning records to caller.
                var result = new List<LogItem>();
                foreach (var idx in await Utilities.RecordsAsync(
                    session,
                    builder.ToString(),
                    args.ToArray()))
                {
                    var dt = idx.GetValue<DateTime>("created");
                    var sd = idx.GetValue<SortedDictionary<string, string>>("meta");
                    result.Add(new LogItem
                    {
                        Id = Convert.ToString(idx.GetValue<Guid>("id")),
                        Created = dt.EnsureUtc(),
                        Type = idx.GetValue<string>("type"),
                        Content = idx.GetValue<string>("content"),
                        Exception = idx.GetValue<string>("exception"),
                        Meta = sd == null ? null : new Dictionary<string, string>(sd),
                    });
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task<long> CountAsync(string content = null)
        {
            // Sanity checking invocation.
            if (content != null)
                throw new HyperlambdaException("The NoSQL data adapter doesn't support content filtering for log items");

            // Creating a cluster session using magic_log keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                // Creating builder to hold our CQL.
                var builder = new StringBuilder("select count(*) from log where tenant = ? and cloudlet = ?");

                // Creating arguments to hold our arguments.
                List<object> args = new List<object>();
                var ids = Utilities.Resolve(_rootResolver);
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);

                // Executing CQL and returning scalar value to caller being count of items.
                var rs = await Utilities.SingleAsync(
                    session,
                    builder.ToString(),
                    args.ToArray());
                return rs.GetValue<long>(0);
            }
        }

        /// <inheritdoc/>
        public Task<IEnumerable<(string When, long Count)>> Timeshift(string content)
        {
            throw new HyperlambdaException("The NoSQL data adapter doesn't support timeshift invocations");
        }

        /// <inheritdoc/>
        public async Task<LogItem> Get(object id)
        {
            // Creating a cluster session using magic_log keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                // Creating builder to hold our CQL.
                var builder = new StringBuilder(
                    "select created as id, toTimestamp(created) as created, type, content, meta, exception from log where tenant = ? and cloudlet = ? and created = ?");

                // Creating arguments to hold our arguments.
                List<object> args = new List<object>();
                var ids = Utilities.Resolve(_rootResolver);
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                args.Add(Guid.Parse(id.ToString()));

                // Executing CQL making sure we get one record returned.
                var row = await Utilities.SingleAsync(
                    session,
                    builder.ToString(),
                    args.ToArray()) ?? throw new HyperlambdaException($"Log item with id of '{id}' was not found");

                // Returning record to caller.
                var dt = row.GetValue<DateTime>("created");
                var sd = row.GetValue<SortedDictionary<string, string>>("meta");
                return new LogItem
                {
                    Id = Convert.ToString(row.GetValue<Guid>("id")),
                    Created = dt.EnsureUtc(),
                    Type = row.GetValue<string>("type"),
                    Content = row.GetValue<string>("content"),
                    Exception = row.GetValue<string>("exception"),
                    Meta = sd == null ? null : new Dictionary<string, string>(sd),
                };
            }
        }

        /// <inheritdoc/>
        public Capabilities Capabilities()
        {
            return new Capabilities
            {
                CanFilter = false,
                CanTimeShift = false,
            };
        }

        #endregion

        #endregion

        #region [ -- Private helper methods and properties -- ]

        async Task InsertLogEntryAsync(
            string type,
            string content,
            Dictionary<string, string> meta,
            string stackTrace)
        {
            // Retrieving IDbConnection to use.
            var shouldLog = false;
            switch (type)
            {
                case "debug":
                    shouldLog = _settings.Level == "debug";
                    break;

                case "info":
                    shouldLog = _settings.Level == "info" || _settings.Level == "debug";
                    break;

                case "error":
                    shouldLog = _settings.Level == "error" || _settings.Level == "info" || _settings.Level == "debug";
                    break;

                case "fatal":
                    shouldLog = _settings.Level == "fatal" || _settings.Level == "error" || _settings.Level == "info" || _settings.Level == "debug";
                    break;

                default:
                    if (_settings.Level != "off")
                        throw new HyperlambdaException($"Configuration error, I only understand 'debug', 'info', 'error', 'fatal', and 'off' as magic:logging:level.");
                    break;
            }

            // Verifying we're supposed to log.
            if (shouldLog)
            {
                var ids = Utilities.Resolve(_rootResolver);
                using (var session = Utilities.CreateSession(_configuration, "magic_log"))
                {
                    // Creating our CQL and argument collection.
                    var builder = new StringBuilder("insert into log (tenant, cloudlet, created, type, content");
                    var tail = new StringBuilder(") values (?, ?, now(), ?, ?");
                    var args = new List<object>
                    {
                        ids.Tenant,
                        ids.Cloudlet,
                        type,
                        content,
                    };

                    // Checking if we've got an exception.
                    if (stackTrace != null)
                    {
                        builder.Append(", exception");
                        args.Add(stackTrace);
                        tail.Append(", ?");
                    }

                    // Checking if we've got meta information.
                    if (meta != null && meta.Any())
                    {
                        builder.Append(", meta");
                        args.Add(meta);
                        tail.Append(", ?");
                    }
                    tail.Append(")");
                    builder.Append(tail.ToString());

                    await Utilities.ExecuteAsync(
                        session,
                        builder.ToString(),
                        args.ToArray());
                }
            }
        }

        #endregion
    }
}
