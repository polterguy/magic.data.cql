/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Cassandra;

namespace magic.data.cql.helpers
{
    /*
     * Helper class for common methods.
     */
    internal static class Utilities
    {
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
        internal static (string Folder, string File) BreakDownFileName(string path)
        {
            var folder = path.Substring(0, path.LastIndexOf('/') + 1).Trim('/');
            if (folder.Length == 0)
                folder = "/";
            else
                folder = "/" + folder.Trim('/') + "/";
            return (folder, path.Substring(path.LastIndexOf('/') + 1));
        }

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        static Cluster _cluster;
        static readonly object _locker = new object();
        internal static ISession CreateSession(IConfiguration configuration, string db = "magic")
        {
            if (_cluster == null)
            {
                lock (_locker)
                {
                    if (_cluster == null)
                        _cluster = Cluster.Builder()
                            .AddContactPoints(configuration["magic:cql:host"] ?? "127.0.0.1")
                            .Build();
                }
            }
            return _cluster.Connect(db);
        }

        #region [ -- Private helper methods -- ]

        static ConcurrentDictionary<string, PreparedStatement> _statements = new ConcurrentDictionary<string, PreparedStatement>();

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