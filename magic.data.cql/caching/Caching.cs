/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using magic.node.contracts;
using magic.node.extensions;
using magic.data.cql.helpers;
using magic.lambda.caching.contracts;

namespace magic.data.cql.caching
{
    /// <inheritdoc/>
    public class Caching : IMagicCache
    {
        readonly IRootResolver _rootResolver;
        readonly IConfiguration _configuration;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="rootResolver">Needed to resolve tenant and cloudlet</param>
        /// <param name="configuration">Needed to create our CQL session</param>
        public Caching (IRootResolver rootResolver, IConfiguration configuration)
        {
            _rootResolver = rootResolver;
            _configuration = configuration;
        }

        /// <inheritdoc/>
        public async Task ClearAsync(string filter = null, bool hidden = false)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<object> GetAsync(string key, bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Creating our CQL.
                var cql = "select key, value from cache where tenant = ? and cloudlet = ? and key = ?";

                // Executing CQL towards session.
                var record = await Utilities.SingleAsync(
                    session,
                    cql, new object[] {
                        ids.Tenant,
                        ids.Cloudlet,
                        ((hidden ? "." : "+") + key),
                    });

                // Returning match to caller if any.
                return record?.GetValue<string>("value");
            }
        }

        /// <inheritdoc/>
        public async Task<object> GetOrCreateAsync(string key, Func<Task<(object, DateTime)>> factory, bool hidden = false)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<KeyValuePair<string, object>>> ItemsAsync(string filter = null, bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Creating our CQL.
                var cql = "select key, value from cache where tenant = ? and cloudlet = ?";

                // Executing CQL towards session.
                var rs = await Utilities.RecordsAsync(
                    session,
                    cql, new object[] {
                        ids.Tenant,
                        ids.Cloudlet
                    });

                // Returning matches to caller.
                List<KeyValuePair<string, object>> result = new List<KeyValuePair<string, object>>();
                foreach (var idx in rs)
                {
                    var key = idx.GetValue<string>("key");
                    if (filter == null || key.StartsWith((hidden ? "." : "+") + filter))
                    {
                        var value = idx.GetValue<string>("value");
                        result.Add(new KeyValuePair<string, object>(key, value));
                    }
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task RemoveAsync(string key, bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Creating our CQL.
                var cql = "delete from cache where tenant = ? and cloudlet = ? and key = ?";

                // Executing CQL towards session.
                await Utilities.ExecuteAsync(
                    session,
                    cql, new object[] {
                        ids.Tenant,
                        ids.Cloudlet,
                        GetKey(key, hidden),
                    });
            }
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(string key, object value, DateTime utcExpiration, bool hidden = false)
        {
            // Sanity checking invocation.
            if (utcExpiration < DateTime.UtcNow)
                throw new HyperlambdaException($"You cannot upsert a new item into your cache with an expiration date that is in the past. Cache key of item that created conflict was '{key}'");

            // Calculating TTL in aseconds.
            var ttl = Convert.ToUInt64((utcExpiration - DateTime.UtcNow).TotalSeconds);
            if (ttl < 1L)
                throw new HyperlambdaException("The minimum expiration time for cache items as 1 second into the future.");

            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Creating our CQL.
                var cql = $"insert into cache (tenant, cloudlet, key, value) values (?, ?, ?, ?) using ttl {ttl}";

                // Executing CQL towards session.
                await Utilities.ExecuteAsync(
                    session,
                    cql,
                    new object[] {
                        ids.Tenant,
                        ids.Cloudlet,
                        GetKey(key, hidden),
                        value.ToString(),
                    });
            }
        }

        #region [ -- Private helper methods -- ]

        static string GetKey(string key, bool hidden)
        {
            // Sanity checking invocation.
            if (string.IsNullOrEmpty(key))
                throw new HyperlambdaException("You cannot reference an item in your cache without providing us with a key.");
            if (key.StartsWith(".") || key.StartsWith("+"))
                throw new HyperlambdaException($"You cannot reference an new item in your cache that starts with a period (.) or a plus (+) - Cache key of item that created conflict was '{key}'");

            // Returning full key value to caller according to visibility.
            return (hidden ? "." : "+") + key;
        }

        #endregion
    }
}
