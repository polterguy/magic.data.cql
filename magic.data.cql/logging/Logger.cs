/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using magic.node.contracts;
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

        /// <inheritdoc/>
        public void Debug(string value)
        {
            InsertLogEntryAsync("debug", value)
                .GetAwaiter()
                .GetResult();
        }

        /// <inheritdoc/>
        public void Error(string value, Exception error = null)
        {
            InsertLogEntryAsync("error", value, error)
                .GetAwaiter()
                .GetResult();
        }

        /// <inheritdoc/>
        public void Error(string value, string stackTrace)
        {
            InsertLogEntryAsync("error", value, null, stackTrace)
                .GetAwaiter()
                .GetResult();
        }

        /// <inheritdoc/>
        public void Fatal(string value, Exception error = null)
        {
            InsertLogEntryAsync("fatal", value, error)
                .GetAwaiter()
                .GetResult();
        }

        /// <inheritdoc/>
        public void Info(string value)
        {
            InsertLogEntryAsync("info", value)
                .GetAwaiter()
                .GetResult();
        }

        /// <inheritdoc/>
        public Task DebugAsync(string value)
        {
            return InsertLogEntryAsync("debug", value);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string value, Exception error = null)
        {
            return InsertLogEntryAsync("error", value, error);
        }

        /// <inheritdoc/>
        public Task ErrorAsync(string value, string stackTrace)
        {
            return InsertLogEntryAsync("error", value, null, stackTrace);
        }

        /// <inheritdoc/>
        public Task FatalAsync(string value, Exception error = null)
        {
            return InsertLogEntryAsync("fatal", value, error);
        }

        /// <inheritdoc/>
        public Task InfoAsync(string value)
        {
            return InsertLogEntryAsync("info", value);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<LogItem>> QueryAsync(int max, object fromId)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select created as id, toTimestamp(created) as created, type, content, exception from log");
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
                    result.Add(new LogItem
                    {
                        Id = Convert.ToString(idx.GetValue<Guid>("id")),
                        Created = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc),
                        Type = idx.GetValue<string>("type"),
                        Content = idx.GetValue<string>("content"),
                        Exception = idx.GetValue<string>("exception"),
                    });
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task<long> CountAsync()
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select count(*) from log");
                builder.Append(" where tenant = ? and cloudlet = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);

                var rs = await Utilities.SingleAsync(
                    session,
                    builder.ToString(),
                    args.ToArray());
                return rs.GetValue<long>(0);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<(string Type, long Count)>> Types()
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);

                var result = new List<(string Type, long Count)>();
                foreach (var idx in await Utilities.RecordsAsync(
                    session,
                    "select type, count(*) as count from log where tenant = ? and cloudlet = ? group by tenant, cloudlet, type",
                    args.ToArray()))
                {
                    var type = idx.GetValue<string>("type");
                    var count = Convert.ToInt64(idx.GetValue<object>("count"));
                    result.Add((type, count));
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<(string When, long Count)>> Timeshift(string content)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                args.Add(content);

                var result = new List<(string When, long Count)>();
                foreach (var idx in await Utilities.RecordsAsync(
                    session,
                    "select day, count(*) as count from log where tenant = ? and cloudlet = ? and content = ? group by tenant, cloudlet, day allow filtering",
                    args.ToArray()))
                {
                    var type = Convert.ToString(idx.GetValue<object>("day"));
                    var count = Convert.ToInt64(idx.GetValue<object>("count"));
                    result.Add((type, count));
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task<LogItem> Get(object id)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_log"))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select created as id, toTimestamp(created) as created, type, content, exception from log");
                builder.Append(" where tenant = ? and cloudlet = ? and created = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                args.Add(Guid.Parse(id.ToString()));

                var result = new List<LogItem>();
                var row = await Utilities.SingleAsync(
                    session,
                    builder.ToString(),
                    args.ToArray());
                var dt = row.GetValue<DateTime>("created");
                return new LogItem
                {
                    Id = Convert.ToString(row.GetValue<Guid>("id")),
                    Created = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc),
                    Type = row.GetValue<string>("type"),
                    Content = row.GetValue<string>("content"),
                    Exception = row.GetValue<string>("exception"),
                };
            }
        }

        #endregion

        #region [ -- Private helper methods and properties -- ]

        async Task InsertLogEntryAsync(
            string type,
            string content,
            Exception error = null, 
            string stackTrace = null)
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
            }

            // Verifying we're supposed to log.
            if (shouldLog)
            {
                var ids = Utilities.Resolve(_rootResolver);
                using (var session = Utilities.CreateSession(_configuration, "magic_log"))
                {
                    var builder = new StringBuilder();
                    builder.Append("insert into log (tenant, cloudlet, created, day, type, content");
                    if (error != null || stackTrace != null)
                        builder.Append(", exception");
                    builder.Append(") values (:tenant, :cloudlet, now(), currentDate(), ?, ?");
                    if (error != null || stackTrace != null)
                        builder.Append(", ?");
                    builder.Append(")");
                    var args = new List<object>
                    {
                        ids.Tenant,
                        ids.Cloudlet,
                        type,
                        content,
                    };
                    if (error != null || stackTrace != null)
                        args.Add(error?.StackTrace ?? stackTrace);
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
