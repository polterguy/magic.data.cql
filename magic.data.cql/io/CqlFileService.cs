/*
 * Magic Cloud, copyright Aista, Ltd and Thomas Hansen. See the attached LICENSE file for details. For license inquiries you can send an email to thomas@ainiro.io
 */

using System.Text;
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                await SaveAsync(
                    session,
                    _rootResolver,
                    destination,
                    await GetFileContent(session, _rootResolver, source));
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                var relPath = Utilities.Relativize(_rootResolver, folder);
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
                        var idxFilename = idx.GetValue<string>("filename");
                        if (!string.IsNullOrEmpty(idxFilename) && (extension == null || idxFilename.EndsWith(extension)))
                            result.Add(_rootResolver.RootFolder + relPath.Substring(1) + idxFilename);
                    }
                    result.Sort();
                    return result;
                }
            }
        }

        /// <inheritdoc />
        public List<string> ListFilesRecursively(string folder, string extension = null)
        {
            return ListFilesRecursivelyAsync(folder, extension).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFilesRecursivelyAsync(string folder, string extension = null)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                var relPath = Utilities.Relativize(_rootResolver, folder);
                var ids = Utilities.Resolve(_rootResolver);
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder, filename from files where tenant = ? and cloudlet = ? and folder like ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath + "%"))
                {
                    var result = new List<string>();
                    foreach (var idx in rs)
                    {
                        var idxFolder = idx.GetValue<string>("folder");
                        var idxFilename = idx.GetValue<string>("filename");
                        if (!string.IsNullOrEmpty(idxFilename) && (extension == null || idxFilename.EndsWith(extension)))
                            result.Add(_rootResolver.RootFolder + idxFolder.Substring(1) + idxFilename);
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                return Encoding.UTF8.GetString(await GetFileContent(session, _rootResolver, path));
            }
        }

        /// <inheritdoc/>
        public IEnumerable<(string Filename, byte[] Content)> LoadRecursively(
            string folder,
            string extension)
        {
            return LoadRecursivelyAsync(folder, extension).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<(string Filename, byte[] Content)>> LoadRecursivelyAsync(
            string folder,
            string extension)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                var relPath = Utilities.Relativize(_rootResolver, folder);
                var ids = Utilities.Resolve(_rootResolver);
                using (var rs = await Utilities.RecordsAsync(
                    session,
                    "select folder, filename, content from files where tenant = ? and cloudlet = ? and folder like ?",
                    ids.Tenant,
                    ids.Cloudlet,
                    relPath + "%"))
                {
                    var result = new List<(string Filename, byte[] Content)>();
                    foreach (var idx in rs)
                    {
                        var idxFolder = idx.GetValue<string>("folder");
                        var idxFilename = idx.GetValue<string>("filename");
                        var idxContent = idx.GetValue<byte[]>("content");
                        if (!string.IsNullOrEmpty(idxFilename) && (extension == null || idxFilename.EndsWith(extension)))
                            result.Add((_rootResolver.RootFolder + idxFolder.Substring(1) + idxFilename, idxContent));
                    }
                    result.Sort((lhs, rhs) => lhs.Filename.CompareTo(rhs.Filename));
                    return result;
                }
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                return await GetFileContent(session, _rootResolver, path);
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
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                await SaveAsync(
                    session,
                    _rootResolver,
                    destination,
                    await GetFileContent(session, _rootResolver, source));
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
            await SaveAsync(path, Encoding.UTF8.GetBytes(content));
        }

        /// <inheritdoc />
        public async Task SaveAsync(string path, byte[] content)
        {
            using (var session = Utilities.CreateSession(_configuration, "magic_files"))
            {
                var relDest = Utilities.BreakDownFileName(_rootResolver, path);
                if (!await CqlFolderService.FolderExists(session, _rootResolver, relDest.Folder))
                    throw new HyperlambdaException($"Destination folder '{relDest.Folder}' doesn't exist");

                await SaveAsync(
                    session,
                    _rootResolver,
                    path,
                    content);
            }
        }

        #region [ -- Internal helper methods -- ]

        /*
         * Returns the content of the specified file to caller.
         */
        internal static async Task<byte[]> GetFileContent(
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
            return rs?.GetValue<byte[]>("content") ?? throw new HyperlambdaException($"File '{rel.File}' doesn't exist");
        }

        /*
         * Common helper method to save file on specified session given specified client and cloudlet ID.
         */
        internal static async Task SaveAsync(
            ISession session,
            IRootResolver rootResolver,
            string path,
            byte[] content)
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