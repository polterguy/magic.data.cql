/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node.contracts;

namespace magic.data.cql.helpers
{
    /*
     * Helper class for common methods.
     */
    internal static class Utilities
    {
        static readonly ConcurrentDictionary<string, Cluster> _clusters = new System.Collections.Concurrent.ConcurrentDictionary<string, Cluster>();
        static ConcurrentDictionary<string, PreparedStatement> _statements = new ConcurrentDictionary<string, PreparedStatement>();

        /*
         * Executes the specified CQL with the specified parameters and returns to caller as a RowSet.
         */
        internal static async Task<RowSet> RecordsAsync(
            ISession session,
            string cql,
            params object[] args)
        {
            return await session.ExecuteAsync(GetStatement(session, cql).Bind(args));
        }

        /*
         * Executes the specified CQL with the specified parameters and returns the first row to caller.
         */
        internal static async Task<Row> SingleAsync(
            ISession session,
            string cql,
            params object[] args)
        {
            using (var rs = await session.ExecuteAsync(GetStatement(session, cql).Bind(args)))
            {
                return rs.FirstOrDefault();
            }
        }

        /*
         * Executes the specified CQL with the specified parameters.
         */
        internal static async Task ExecuteAsync(
            ISession session,
            string cql,
            params object[] args)
        {
            await session.ExecuteAsync(GetStatement(session, cql).Bind(args));
        }

        /*
         * Breaks down the specified path into its folder value and its file value.
         */
        internal static (string Folder, string File) BreakDownFileName(IRootResolver rootResolver, string path)
        {
            path = path.Substring(rootResolver.RootFolder.Length - 1);
            var folder = path.Substring(0, path.LastIndexOf('/') + 1).Trim('/');
            if (folder.Length == 0)
                folder = "/";
            else
                folder = "/" + folder + "/";
            return (folder, path.Substring(path.LastIndexOf('/') + 1));
        }

        internal static ISession CreateSession(IConfiguration configuration, string keySpace)
        {
            var connection = GetDefaultConnection(configuration, keySpace);
            return _clusters.GetOrAdd(connection.Cluster, (key) =>
            {
                return Cluster.Builder()
                    .AddContactPoints(key)
                    .Build();
            }).Connect(connection.KeySpace);
        }

        /*
         * Returns the relative path of the specified absolute path.
         */
        internal static string Relativize(IRootResolver rootResolver, string path)
        {
            return path.Substring(rootResolver.RootFolder.Length - 1);
        }

        /*
         * Returns the tenant ID and the cloudlet ID given the specified root resolver.
         */
        internal static (string Tenant, string Cloudlet) Resolve(IRootResolver rootResolver)
        {
            var splits = rootResolver.RootFolder.Split(new char[] {'/'}, StringSplitOptions.RemoveEmptyEntries);
            return (splits.First(), string.Join("/", splits.Skip(1)));
        }

        #region [ -- Private helper methods -- ]

        /*
         * Returns the default Cluster and keyspace according to configuration settings.
         */
        static (string Cluster, string KeySpace) GetDefaultConnection(
            IConfiguration configuration,
            string keySpace)
        {
            return (configuration["magic:cql:generic:host"] ?? "127.0.0.1", keySpace);
        }

        /*
         * Returns a prepared statement from the specified cql.
         */
        static PreparedStatement GetStatement(ISession session, string cql)
        {
            return _statements.GetOrAdd(cql, (key) => session.Prepare(cql));
        }

        #endregion
    }
}