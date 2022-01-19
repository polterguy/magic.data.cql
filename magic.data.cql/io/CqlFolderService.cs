/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
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
        public Task CopyAsync(string source, string destination)
        {
            throw new NotImplementedException();
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
                await Utilities.ExecuteAsync(
                    session,
                    "insert into files (cloudlet, folder, filename) values (?, ?, '')",
                    _rootResolver.DynamicFiles,
                    _rootResolver.RelativePath(path).TrimEnd('/') + "/");
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
            var relPath = _rootResolver.RelativePath(path);
            using (var session = Utilities.CreateSession(_configuration))
            {
                var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder from files where cloudlet = ? and folder like ?",
                    _rootResolver.DynamicFiles,
                    relPath + '%');
                foreach (var idx in rs)
                {
                    await Utilities.ExecuteAsync(
                        session,
                        "delete from files where cloudlet = ? and folder = ?",
                        _rootResolver.DynamicFiles,
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
                return await Utilities.SingleAsync(
                    session,
                    "select folder from files where cloudlet = ? and folder = ? and filename = ''",
                    _rootResolver.DynamicFiles,
                    _rootResolver.RelativePath(path)) == null ? false : true;
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
            var relativeFolder = _rootResolver.RelativePath(folder);
            using (var session = Utilities.CreateSession(_configuration))
            {
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder from files where cloudlet = ? and folder like ? and filename = ''",
                    _rootResolver.DynamicFiles,
                    relativeFolder + "%"))
                {
                    var result = new List<string>();
                    foreach (var idx in rs.GetRows())
                    {
                        var idxFolder = idx.GetValue<string>("folder").TrimEnd('/');
                        if (idxFolder.StartsWith(relativeFolder) && idxFolder.LastIndexOf("/") == relativeFolder.LastIndexOf("/"))
                            result.Add(_rootResolver.DynamicFiles.TrimEnd('/') + idxFolder + "/");
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
        public Task MoveAsync(string source, string destination)
        {
            throw new NotImplementedException();
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
            return await Utilities.SingleAsync(
                session,
                "select folder from files where cloudlet = ? and folder = ? and filename = ''",
                rootResolver.DynamicFiles,
                path) == null ? false : true;
        }

        #endregion
    }
}