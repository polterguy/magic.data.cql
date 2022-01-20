/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node;
using magic.node.contracts;
using magic.node.extensions;

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
            using (await session.ExecuteAsync(GetStatement(session, cql).Bind(args))) { }
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

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        internal static ISession CreateSession(string cluster, string keyspace)
        {
            return _clusters.GetOrAdd(cluster, (key) =>
            {
                return Cluster.Builder()
                    .AddContactPoints(key)
                    .Build();
            }).Connect(keyspace);
        }

        internal static ISession CreateSession(IConfiguration configuration)
        {
            var connection = GetDefaultConnection(configuration);
            return CreateSession(connection.Cluster, connection.KeySpace);
        }

        /*
         * Returns connection settings for Cluster and keyspace to caller given the  specified node.
         */
        internal static (string Cluster, string KeySpace) GetConnectionSettings(Node node)
        {
            var value = node.GetEx<string>();
            if (value.StartsWith("[") && value.EndsWith("]"))
            {
                var splits = value.Substring(1, value.Length - 2).Split('|');
                if (splits.Count() != 2)
                    throw new HyperlambdaException($"I don't understand how to connect to a CQL database using '{value}'");
                return (splits[0], splits[1]);
            }
            else
            {
                return ("generic", value);
            }
        }

        #region [ -- Private helper methods -- ]

        /*
         * Returns the default Cluster and keyspace according to configuration settings.
         */
        static (string Cluster, string KeySpace) GetDefaultConnection(IConfiguration configuration)
        {
            return (configuration["magic:cql:generic:host"] ?? "127.0.0.1", "magic");
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