/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.Linq;
using System.Threading.Tasks;
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
        public static async Task<RowSet> RecordsAsync(
            ISession session,
            string cql,
            params (string, object)[] args)
        {
            return await session.ExecuteAsync(new SimpleStatement(args.ToDictionary(x => x.Item1, x => x.Item2), cql));
        }

        /*
         * Executes the specified CQL with the specified parameters and returns the first row to caller.
         */
        public static async Task<Row> SingleAsync(
            ISession session,
            string cql,
            params (string, object)[] args)
        {
            var rs = await session.ExecuteAsync(new SimpleStatement(args.ToDictionary(x => x.Item1, x => x.Item2), cql));
            return rs.FirstOrDefault();
        }

        /*
         * Executes the specified CQL with the specified parameters.
         */
        public static async Task ExecuteAsync(
            ISession session,
            string cql,
            params (string, object)[] args)
        {
            await session.ExecuteAsync(new SimpleStatement(args.ToDictionary(x => x.Item1, x => x.Item2), cql));
        }

        /*
         * Breaks down the specified path into its folder value and its file value.
         */
        internal static (string Folder, string File) BreakDownPath(string path)
        {
            return (path.Substring(0, path.LastIndexOf('/') + 1), path.Substring(path.LastIndexOf('/') + 1));
        }

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        internal static ISession CreateSession(IConfiguration configuration, string db = "magic")
        {
            var cluster = Cluster.Builder()
                .AddContactPoints(configuration["magic:cql:host"] ?? "127.0.0.1")
                .Build();
            return cluster.Connect(db);
        }
    }
}