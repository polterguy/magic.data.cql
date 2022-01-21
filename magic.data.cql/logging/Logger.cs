/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Text;
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
        public async Task<IEnumerable<LogItem>> QueryAsync(int max, object fromId, string query = null)
        {
            if (!string.IsNullOrEmpty(query) && (query.Contains("%") || query.Contains("?")))
                throw new HyperlambdaException($"You cannot filter log items with wild cards");

            using (var session = Utilities.CreateSession(_configuration))
            {
                var builder = new StringBuilder();
                var ids = Utilities.Resolve(_rootResolver);
                List<object> args = new List<object>();
                builder.Append("select created as id, toTimestamp(created) as created, type, content, exception from log_entries");
                builder.Append(" where tenant = ? and cloudlet = ?");
                args.Add(ids.Tenant);
                args.Add(ids.Cloudlet);
                if (fromId != null || !string.IsNullOrEmpty(query))
                {
                    if (fromId != null)
                    {
                        builder.Append($" and created < ?");
                        args.Add(Guid.Parse(fromId.ToString()));
                        if (!string.IsNullOrEmpty(query))
                        {
                            builder.Append(" and content like ?");
                            args.Add(query + "%");
                        }
                    }
                    else
                    {
                            builder.Append(" and content like ?");
                            args.Add(query + "%");
                    }
                }
                builder.Append($" order by created desc limit {max} allow filtering");

                var result = new List<LogItem>();
                foreach (var idx in await Utilities.RecordsAsync(
                    session,
                    builder.ToString(),
                    args.ToArray()))
                {
                    result.Add(new LogItem
                    {
                        Id = idx.GetValue<Guid>("id"),
                        Created = idx.GetValue<DateTime>("created"),
                        Type = idx.GetValue<string>("type"),
                        Content = idx.GetValue<string>("content"),
                        Exception = idx.GetValue<string>("exception"),
                    });
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public Task<LogItem> Get(object id)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public Task<long> CountAsync(string query = null)
        {
            throw new NotImplementedException();
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
                using (var session = Utilities.CreateSession(_configuration))
                {
                    var builder = new StringBuilder();
                    builder.Append("insert into log_entries (tenant, cloudlet, created, type, content");
                    if (error != null || stackTrace != null)
                        builder.Append(", exception");
                    builder.Append(") values (:tenant, :cloudlet, now(), ?, ?");
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
