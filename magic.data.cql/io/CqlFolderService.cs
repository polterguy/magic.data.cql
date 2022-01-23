/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node.contracts;
using magic.data.cql.helpers;

namespace magic.data.cql.io
{
    /// <summary>
    /// Folder service for Magic storing folders in ScyllaDB.
    /// </summary>
    public class CqlFolderService : IFolderService
    {
        readonly IConfiguration _configuration;
        readonly IRootResolver _rootResolver;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        public CqlFolderService(IConfiguration configuration, IRootResolver rootResolver)
        {
            _configuration = configuration;
            _rootResolver = rootResolver;
        }

        /// <inheritdoc />
        public void Copy(string source, string destination)
        {
            CopyAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task CopyAsync(string source, string destination)
        {
            await CopyMoveImplementation(source, destination, false);
        }

        /// <inheritdoc />
        public void Create(string path)
        {
            CreateAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task CreateAsync(string path)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                await Utilities.ExecuteAsync(
                    session,
                    "insert into files (tenant, cloudlet, folder, filename) values (?, ?, ?, '')",
                    ids.Tenant,
                    ids.Cloudlet,
                    Utilities.Relativize(_rootResolver, path).TrimEnd('/') + "/");
            }
        }

        /// <inheritdoc />
        public void Delete(string path)
        {
            DeleteAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task DeleteAsync(string path)
        {
            var relPath = Utilities.Relativize(_rootResolver, path);
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder from files where tenant = ? and cloudlet = ? and folder like ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath + '%');
                foreach (var idx in rs)
                {
                    await Utilities.ExecuteAsync(
                        session,
                        "delete from files where tenant = ? and cloudlet = ? and folder = ?",
                        ids.Tenant,
                        ids.Cloudlet,
                        relPath);
                }
            }
        }

        /// <inheritdoc />
        public bool Exists(string path)
        {
            return ExistsAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<bool> ExistsAsync(string path)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                return await Utilities.SingleAsync(
                    session,
                    "select folder from files where tenant = ? and cloudlet = ? and folder = ? and filename = ''",
                    ids.Tenant,
                    ids.Cloudlet,
                    Utilities.Relativize(_rootResolver, path)) != null;
            }
        }

        /// <inheritdoc />
        public List<string> ListFolders(string folder)
        {
            return ListFoldersAsync(folder).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFoldersAsync(string folder)
        {
            var relativeFolder = Utilities.Relativize(_rootResolver, folder);
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder from files where tenant = ? and cloudlet = ? and folder like ? and filename = ''",
                    ids.Tenant,
                    ids.Cloudlet,
                    relativeFolder + "%"))
                {
                    var result = new List<string>();
                    foreach (var idx in rs.GetRows())
                    {
                        var idxFolder = idx.GetValue<string>("folder").TrimEnd('/');
                        if (idxFolder.StartsWith(relativeFolder) && idxFolder.LastIndexOf("/") == relativeFolder.LastIndexOf("/"))
                            result.Add(_rootResolver.RootFolder.TrimEnd('/') + idxFolder + "/");
                    }
                    result.Sort();
                    return result;
                }
            }
        }

        /// <inheritdoc />
        public List<string> ListFoldersRecursively(string folder)
        {
            return ListFoldersAsync(folder).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFoldersRecursivelyAsync(string folder)
        {
            var relativeFolder = Utilities.Relativize(_rootResolver, folder);
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder from files where tenant = ? and cloudlet = ? and folder like ? and filename = ''",
                    ids.Tenant,
                    ids.Cloudlet,
                    relativeFolder + "%"))
                {
                    var result = new List<string>();
                    foreach (var idx in rs.GetRows())
                    {
                        var idxFolder = idx.GetValue<string>("folder");
                        if (idxFolder != relativeFolder)
                            result.Add(_rootResolver.RootFolder.TrimEnd('/') + idxFolder);
                    }
                    result.Sort();
                    return result;
                }
            }
        }

        /// <inheritdoc />
        public void Move(string source, string destination)
        {
            MoveAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task MoveAsync(string source, string destination)
        {
            await CopyMoveImplementation(source, destination, true);
        }

        #region [ -- Internal helper methods -- ]

        /*
         * Returns true if folder exists.
         */
        internal static async Task<bool> FolderExists(
            ISession session,
            IRootResolver rootResolver,
            string path)
        {
            var ids = Utilities.Resolve(rootResolver);
            return await Utilities.SingleAsync(
                session,
                "select folder from files where tenant = ? and cloudlet = ? and folder = ? and filename = ''",
                ids.Tenant,
                ids.Cloudlet,
                path) != null;
        }

        #endregion

        #region [ -- Private helper methods -- ]

        /*
         * Implementation of copy/move.
         */
        async Task CopyMoveImplementation(string source, string destination, bool isMove)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var relSrc = Utilities.Relativize(_rootResolver, source).TrimEnd('/') + "/";
                var ids = Utilities.Resolve(_rootResolver);
                var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder, filename, content from files where tenant = ? and cloudlet = ? and folder like ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relSrc + "%");
                var relDest = Utilities.Relativize(_rootResolver, destination).TrimEnd('/') + "/";
                foreach (var idx in rs)
                {
                    var idxFolder = idx.GetValue<string>("folder");
                    var idxFilename = idx.GetValue<string>("filename");
                    var idxContent = idx.GetValue<byte[]>("content");
                    if (isMove)
                    {
                        await Utilities.ExecuteAsync(
                            session,
                            "delete from files where tenant = ? and cloudlet = ? and folder = ? and filename = ?",
                            ids.Tenant,
                            ids.Cloudlet,
                            idxFolder,
                            idxFilename);
                    }
                    idxFolder = relDest + idxFolder.Substring(relSrc.Length);
                    await Utilities.ExecuteAsync(
                        session,
                        "insert into files (tenant, cloudlet, folder, filename, content) values (?, ?, ?, ?, ?)",
                        ids.Tenant,
                        ids.Cloudlet,
                        idxFolder,
                        idxFilename,
                        idxContent);
                }
            }
        }

        #endregion
    }
}