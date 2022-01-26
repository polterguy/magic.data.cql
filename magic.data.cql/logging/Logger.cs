/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Text;
using System.Globalization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using magic.node.contracts;
using magic.node.extensions;
using magic.data.cql.helpers;
using magic.signals.contracts;
using magic.lambda.logging.contracts;

namespace magic.data.cql.logging
{
    /// <inheritdoc/>
    public class Logger : ILogger, ILogQuery
    {
        readonly ISignaler _signaler;
        readonly IRootResolver _rootResolver;
        readonly IConfiguration _configuration;
        readonly IMagicConfiguration _magicConfiguration;

        /// <summary>
        /// Constructs a new instance of the default ILogger implementation.
        /// </summary>
        /// <param name="signaler">ISignaler implementation.</param>
        /// <param name="rootResolver">Needed to figure out tenant and cloudlet IDs.</param>
        /// <param name="configuration">Configuration object.</param>
        /// <param name="magicConfiguration">Configuration object.</param>
        public Logger(
            ISignaler signaler,
            IRootResolver rootResolver,
            IConfiguration configuration,
            IMagicConfiguration magicConfiguration)
        {
            _signaler = signaler;
            _rootResolver = rootResolver;
            _configuration = configuration;
            _magicConfiguration = magicConfiguration;
        }

        #region [ -- Interface implementations -- ]

        #region [ -- ILogger interface implementation -- ]

        /// <inheritdoc/>
        public Task DebugAsync(string content)
        {
            return InsertLogEntryAsync(_signaler, "debug", content, null, null);
        }

        /// <inheritdoc/>
        public Task DebugAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync(_signaler, "debug", content, meta, null);
        }

        /// <inheritdoc/>
        public Task InfoAsync(string content)
        {
            return InsertLogEntryAsync(_signaler, "info", content, null, null);
        }

        /// <inheritdoc/>
        public Task InfoAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync(_signaler, "info", content, meta, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content)
        {
            return InsertLogEntryAsync(_signaler, "error", content, null, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync(_signaler, "error", content, meta, null);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, string stackTrace)
        {
            return InsertLogEntryAsync(_signaler, "error", content, null, stackTrace);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string content, Dictionary<string, string> meta, string stackTrace)
        {
            return InsertLogEntryAsync(_signaler, "error", content, meta, stackTrace);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content)
        {
            return InsertLogEntryAsync(_signaler, "fatal", content, null, null);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, Dictionary<string, string> meta)
        {
            return InsertLogEntryAsync(_signaler, "fatal", content, meta, null);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, string stackTrace)
        {
            return InsertLogEntryAsync(_signaler, "fatal", content, null, stackTrace);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string content, Dictionary<string, string> meta, string stackTrace)
        {
            return InsertLogEntryAsync(_signaler, "fatal", content, meta, stackTrace);
        }

        #endregion

        #region [ -- ILogQuery interface implementation -- ]

        /// <inheritdoc/>
        public async Task<IEnumerable<LogItem>> QueryAsync(int max, object fromId, string content = null)
        {
            // Sanity checking invocation.
            if (content != null)
                throw new HyperlambdaException("The NoSQL data adapter doesn't support content filtering for log items");

            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select created as id, toTimestamp(created) as created, type, content, exception, meta from log");
                builder.Append(" where tenant = ? and cloudlet = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                if (fromId != null)
                {
                    builder.Append($" and created < ?");
                    args.Add(Guid.Parse(fromId.ToString()));
                }
                builder.Append($" order by created desc limit {max}");

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
                        Created = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc),
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

            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                var table = content == null ? "log" : "log_content_view";
                builder.Append($"select count(*) from {table}");
                builder.Append(" where tenant = ? and cloudlet = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                if (!string.IsNullOrEmpty(content))
                {
                    builder.Append(" and content = ?");
                    args.Add(content);
                }

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
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select created as id, toTimestamp(created) as created, type, content, meta, exception from log");
                builder.Append(" where tenant = ? and cloudlet = ? and created = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                args.Add(Guid.Parse(id.ToString()));

                var row = await Utilities.SingleAsync(
                    session,
                    builder.ToString(),
                    args.ToArray());
                var dt = row.GetValue<DateTime>("created");
                var sd = row.GetValue<SortedDictionary<string, string>>("meta");
                return new LogItem
                {
                    Id = Convert.ToString(row.GetValue<Guid>("id")),
                    Created = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc),
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
            ISignaler signaler,
            string type,
            string content,
            Dictionary<string, string> meta,
            string stackTrace)
        {
            // Retrieving IDbConnection to use.
            var level = _magicConfiguration["magic:logging:level"] ?? "debug";
            var shouldLog = false;
            switch (type)
            {
                case "debug":
                    shouldLog = level == "debug";
                    break;

                case "info":
                    shouldLog = level == "info" || level == "debug";
                    break;

                case "error":
                    shouldLog = level == "error" || level == "info" || level == "debug";
                    break;

                case "fatal":
                    shouldLog = level == "fatal" || level == "error" || level == "info" || level == "debug";
                    break;

                default:
                    if (level != "off")
                        throw new HyperlambdaException($"Configuration error, I only understand 'debug', 'info', 'error', 'fatal', and 'off' as magic:logging:level.");
                    break;
            }

            // Verifying we're supposed to log.
            if (shouldLog)
            {
                var ids = Utilities.Resolve(_rootResolver);
                using (var session = Utilities.CreateSession(_configuration, "magic_log"))
                {
                    var builder = new StringBuilder();
                    builder.Append("insert into log (tenant, cloudlet, created, day, type, content");
                    if (stackTrace != null)
                        builder.Append(", exception");
                    if (meta != null && meta.Count > 0)
                        builder.Append(", meta");
                    builder.Append(") values (?, ?, now(), ?, ?, ?");
                    if (stackTrace != null)
                        builder.Append(", ?");
                    if (meta != null && meta.Count > 0)
                        builder.Append(", ?");
                    builder.Append(")");
                    var args = new List<object>
                    {
                        ids.Tenant,
                        ids.Cloudlet,
                        DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                        type,
                        content,
                    };
                    if (stackTrace != null)
                        args.Add(stackTrace);
                    if (meta != null && meta.Count > 0)
                    {
                        args.Add(meta);
                    }
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
