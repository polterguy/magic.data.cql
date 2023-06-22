/*
 * Magic Cloud, copyright Aista, Ltd and Thomas Hansen. See the attached LICENSE file for details. For license inquiries you can send an email to thomas@ainiro.io
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Cassandra;
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
        public async Task ClearAsync(
            string filter = null,
            bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Iterating through each item in cache
                foreach (var idx in await IterateAsync(session, ids.Tenant, ids.Cloudlet))
                {
                    // Retrieving key and verifying this is a match.
                    var key = idx.GetValue<string>("key");
                    if (IsMatch(key, filter, hidden))
                    {
                        // Deleting item.
                        await Utilities.ExecuteAsync(
                            session,
                            "delete from cache where tenant = ? and cloudlet = ? and key = ?",
                            new object[] {
                                ids.Tenant,
                                ids.Cloudlet,
                                key,
                            });
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task<string> GetAsync(
            string key,
            bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Invoking helper method to return cache item.
                return await GetAsync(session, ids.Tenant, ids.Cloudlet, hidden, key);
            }
        }

        /// <inheritdoc/>
        public async Task<string> GetOrCreateAsync(
            string key,
            Func<Task<(string, DateTime)>> factory,
            bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Invoking helper method to return cache item.
                var cached = await GetAsync(
                    session,
                    ids.Tenant,
                    ids.Cloudlet,
                    hidden,
                    key);
                if (cached != null)
                    return cached;

                // Invoking factory method.
                var created = await factory();

                // Figuring out TTL value of newly created item.
                var ttl = created.Item2 > DateTime.UtcNow.AddYears(1) ?
                    -1 :
                    Convert.ToInt32((created.Item2 - DateTime.UtcNow).TotalSeconds);

                // Upserting item.
                await UpsertAsync(
                    session,
                    ids.Tenant,
                    ids.Cloudlet,
                    key,
                    hidden,
                    ttl,
                    created.Item1);

                // Returning item to caller.
                return created.Item1;
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<KeyValuePair<string, string>>> ItemsAsync(
            string filter = null,
            bool hidden = false)
        {
            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Returning matches to caller.
                var result = new List<KeyValuePair<string, string>>();
                foreach (var idx in await IterateAsync(session, ids.Tenant, ids.Cloudlet))
                {
                    var key = idx.GetValue<string>("key");
                    if (IsMatch(key, filter, hidden))
                        result.Add(new KeyValuePair<string, string>(key.Substring(1), idx.GetValue<string>("value")));
                }
                return result;
            }
        }

        /// <inheritdoc/>
        public async Task RemoveAsync(
            string key,
            bool hidden = false)
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
                    cql,
                    new object[] {
                        ids.Tenant,
                        ids.Cloudlet,
                        GetKey(key, hidden),
                    });
            }
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(
            string key,
            string value,
            DateTime utcExpiration,
            bool hidden = false)
        {
            // Sanity checking invocation.
            if (utcExpiration < DateTime.UtcNow.AddSeconds(1))
                throw new HyperlambdaException($"You cannot upsert a new item into your cache with an expiration date that is in the past. Cache key of item that created conflict was '{key}'");

            // Calculating TTL in aseconds.
            var ttl = utcExpiration > DateTime.UtcNow.AddYears(1) ?
                -1 :
                Convert.ToInt32((utcExpiration - DateTime.UtcNow).TotalSeconds);

            // Creating a cluster session using magic_cache keyspace.
            using (var session = Utilities.CreateSession(_configuration, "magic_cache"))
            {
                // Figuring out tenant and cloudlet values.
                var ids = Utilities.Resolve(_rootResolver);

                // Invoking helper method to upsert item.
                await UpsertAsync(
                    session,
                    ids.Tenant,
                    ids.Cloudlet,
                    key,
                    hidden,
                    ttl,
                    value);
            }
        }

        #region [ -- Private helper methods -- ]

        /*
         * Helper method to upsert item.
         */
        static async Task UpsertAsync(
            ISession session,
            string tenant,
            string cloudlet,
            string key,
            bool hidden,
            int ttl,
            string value)
        {
            // Creating our CQL.
            var cql = ttl == -1 ? 
                "insert into cache (tenant, cloudlet, key, value) values (?, ?, ?, ?)" :
                $"insert into cache (tenant, cloudlet, key, value) values (?, ?, ?, ?) using ttl {ttl}";

            // Executing CQL towards session.
            await Utilities.ExecuteAsync(
                session,
                cql,
                new object[] {
                    tenant,
                    cloudlet,
                    GetKey(key, hidden),
                    value.ToString(),
                });
        }

        /*
         * Returning cache item from the specified session with the given values.
         */
        static async Task<string> GetAsync(
            ISession session,
            string tenant,
            string cloudlet,
            bool hidden,
            string key)
        {
            // Creating our CQL.
            var cql = "select value from cache where tenant = ? and cloudlet = ? and key = ?";

            // Executing CQL towards session.
            var record = await Utilities.SingleAsync(
                session,
                cql,
                new object[] {
                    tenant,
                    cloudlet,
                    GetKey(key, hidden),
                });

            // Returning match to caller if any.
            return record?.GetValue<string>("value");
        }

        /*
         * Helper method to iterate all cache items belonging to tenant/cloudlet combination.
         */
        static async Task<RowSet> IterateAsync(
            ISession session,
            string tenant,
            string cloudlet)
        {
            // Creating our CQL.
            var cql = "select key, value from cache where tenant = ? and cloudlet = ?";

            // Executing CQL towards session.
            return await Utilities.RecordsAsync(
                session,
                cql,
                new object[] {
                    tenant,
                    cloudlet
                });
        }

        /*
         * Helper method to sanity check and create the key correctly according to whether
         * it's a hidden item or not.
         */
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

        /*
         * Returns true if specified key is matching filter and hidden condition.
         */
        static bool IsMatch(string key, string filter, bool hidden)
        {
            filter = (hidden ? "." : "+") + filter;
            return key.StartsWith(filter);
        }

        #endregion
    }
}
