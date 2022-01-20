/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node.contracts;
using magic.node.extensions;
using magic.data.cql.helpers;

namespace magic.data.cql.io
{
    /// <summary>
    /// File service for Magic storing files in ScyllaDB.
    /// </summary>
    public class CqlFileService : IFileService
    {
        readonly IConfiguration _configuration;
        readonly IRootResolver _rootResolver;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        public CqlFileService(IConfiguration configuration, IRootResolver rootResolver)
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
            using (var session = Utilities.CreateSession(_configuration))
            {
                var relDest = Utilities.Relativize(_rootResolver, destination);
                if (!await CqlFolderService.FolderExists(session, _rootResolver, relDest))
                    throw new HyperlambdaException($"Destination folder '{relDest}' doesn't exist");

                await SaveAsync(session, _rootResolver, destination, await GetFileContent(session, _rootResolver, source));
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
            using (var session = Utilities.CreateSession(_configuration))
            {
                var ids = Utilities.Resolve(_rootResolver);
                var relPath = Utilities.BreakDownFileName(_rootResolver, path);
                await Utilities.ExecuteAsync(
                    session,
                    "delete from files where tenant = ? and cloudlet = ? and folder = ? and filename = ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath.Folder,
                    relPath.File);
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
                var relPath = Utilities.BreakDownFileName(_rootResolver, path);
                var row = await Utilities.SingleAsync(
                    session,
                    "select filename from files where tenant = ? and cloudlet = ? and folder = ? and filename = ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath.Folder,
                    relPath.File);
                return row != null;
            }
        }

        /// <inheritdoc />
        public List<string> ListFiles(string folder, string extension = null)
        {
            return ListFilesAsync(folder, extension).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFilesAsync(string folder, string extension = null)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var relPath = Utilities.Relativize(_rootResolver, folder);
                if (!await CqlFolderService.FolderExists(session, _rootResolver, relPath))
                    throw new HyperlambdaException($"Folder '{relPath}' doesn't exist");

                var ids = Utilities.Resolve(_rootResolver);
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select filename from files where tenant = ? and cloudlet = ? and folder = ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath))
                {
                    var result = new List<string>();
                    foreach (var idx in rs)
                    {
                        var idxFile = idx.GetValue<string>("filename");
                        if (idxFile != "" && (extension == null || idxFile.EndsWith(extension)))
                            result.Add(_rootResolver.RootFolder + relPath.Substring(1) + idxFile);
                    }
                    result.Sort();
                    return result;
                }
            }
        }

        /// <inheritdoc />
        public string Load(string path)
        {
            return LoadAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<string> LoadAsync(string path)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                return await GetFileContent(session, _rootResolver, path);
            }
        }

        /// <inheritdoc />
        public byte[] LoadBinary(string path)
        {
            return LoadBinaryAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<byte[]> LoadBinaryAsync(string path)
        {
            return Convert.FromBase64String(await LoadAsync(path));
        }

        /// <inheritdoc />
        public void Move(string source, string destination)
        {
            MoveAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task MoveAsync(string source, string destination)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var relDest = Utilities.BreakDownFileName(_rootResolver, destination);
                if (!await CqlFolderService.FolderExists(session, _rootResolver, relDest.Folder))
                    throw new HyperlambdaException($"Destination folder '{relDest.Folder}' doesn't exist");

                await SaveAsync(session, _rootResolver, destination, await GetFileContent(session, _rootResolver, source));

                var ids = Utilities.Resolve(_rootResolver);
                var relSrc = Utilities.BreakDownFileName(_rootResolver, source);
                await Utilities.ExecuteAsync(
                    session,
                    "delete from files where tenant = ? and cloudlet = ? and folder = ? and filename = ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relSrc.Folder,
                    relSrc.File);
            }
        }

        /// <inheritdoc />
        public void Save(string path, string content)
        {
            SaveAsync(path, content).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public void Save(string path, byte[] content)
        {
            SaveAsync(path, content).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task SaveAsync(string path, string content)
        {
            using (var session = Utilities.CreateSession(_configuration))
            {
                var relDest = Utilities.BreakDownFileName(_rootResolver, path);
                if (!await CqlFolderService.FolderExists(session, _rootResolver, relDest.Folder))
                    throw new HyperlambdaException($"Destination folder '{relDest.Folder}' doesn't exist");

                await SaveAsync(session, _rootResolver, path, content);
            }
        }

        /// <inheritdoc />
        public async Task SaveAsync(string path, byte[] content)
        {
            await SaveAsync(path, Convert.ToBase64String(content));
        }

        #region [ -- Internal helper methods -- ]

        /*
         * Returns the content of the specified file to caller.
         */
        internal static async Task<string> GetFileContent(
            ISession session,
            IRootResolver rootResolver,
            string path)
        {
            var rel = Utilities.BreakDownFileName(rootResolver, path);
            var ids = Utilities.Resolve(rootResolver);
            var rs = await Utilities.SingleAsync(
                session,
                "select content from files where tenant = ? and cloudlet = ? and folder = ? and filename = ?",
                ids.Tenant,
                ids.Cloudlet,
                rel.Folder,
                rel.File);
            return rs?.GetValue<string>("content") ?? throw new HyperlambdaException($"File '{rel.File}' doesn't exist");
        }

        #endregion

        #region [ -- Private helper methods -- ]

        /*
         * Common helper method to save file on specified session given specified client and cloudlet ID.
         */
        internal static async Task SaveAsync(
            ISession session,
            IRootResolver rootResolver,
            string path,
            string content)
        {
            var relPath = Utilities.BreakDownFileName(rootResolver, path);
            var ids = Utilities.Resolve(rootResolver);
            await Utilities.ExecuteAsync(
                session,
                "insert into files (tenant, cloudlet, folder, filename, content) values (?, ?, ?, ?, ?)",
                ids.Tenant,
                ids.Cloudlet,
                relPath.Folder,
                relPath.File,
                content);
        }

        #endregion
    }
}