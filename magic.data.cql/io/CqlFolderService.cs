/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node.contracts;

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
            using (var session = CqlFileService.CreateSession(_configuration))
            {
                var cql = "insert into files (cloudlet, folder, filename) values (:cloudlet, :folder, '')";
                var args = new Dictionary<string, object>
                {
                    { "cloudlet", _rootResolver.DynamicFiles },
                    { "folder", _rootResolver.RelativePath(path) },
                };
                await session.ExecuteAsync(new SimpleStatement(args, cql));
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
            var relativePath = _rootResolver.RelativePath(path);
            using (var session = CqlFileService.CreateSession(_configuration))
            {
                // Deleting main folder and all sub-folders.
                var cql = "select folder from files where cloudlet = :cloudlet and filename = ''";
                var args = new Dictionary<string, object>
                {
                    { "cloudlet", _rootResolver.DynamicFiles },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                foreach (var idx in rs)
                {
                    var idxFolder = idx.GetValue<string>("folder");
                    if (idxFolder.StartsWith(relativePath))
                    {
                        cql = "delete from files where cloudlet = :cloudlet and folder = :folder and filename = ''";
                        args = new Dictionary<string, object>
                        {
                            { "cloudlet", _rootResolver.DynamicFiles },
                            { "folder", idxFolder },
                        };
                        await session.ExecuteAsync(new SimpleStatement(args, cql));
                    }
                }

                // Deleting all files within folder.
                cql = "select filename from files where cloudlet = :cloudlet";
                args = new Dictionary<string, object>
                {
                    { "cloudlet", _rootResolver.DynamicFiles },
                };
                rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                foreach (var idx in rs)
                {
                    var idxFile = idx.GetValue<string>("filename");
                    if (idxFile.StartsWith(relativePath))
                    {
                        cql = "delete from files where cloudlet = :cloudlet and filename = :filename";
                        args = new Dictionary<string, object>
                        {
                            { "cloudlet", _rootResolver.DynamicFiles },
                            { "filename", idxFile },
                        };
                        await session.ExecuteAsync(new SimpleStatement(args, cql));
                    }
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
            using (var session = CqlFileService.CreateSession(_configuration))
            {
                var cql = "select folder from files where cloudlet = :cloudlet and folder = :folder and filename = ''";
                var args = new Dictionary<string, object>
                {
                    { "cloudlet", _rootResolver.DynamicFiles },
                    { "folder", _rootResolver.RelativePath(path) },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var row = rs.FirstOrDefault();
                return row == null ? false : true;
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
            using (var session = CqlFileService.CreateSession(_configuration))
            {
                var cql = "select folder from files where cloudlet = :cloudlet and folder like :folder and filename = ''";
                var args = new Dictionary<string, object>
                {
                    { "cloudlet", _rootResolver.DynamicFiles },
                    { "folder", _rootResolver.RelativePath(folder) + "%" }
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var result = new List<string>();
                foreach (var idx in rs.GetRows())
                {
                    var idxFolder = idx.GetValue<string>("folder").TrimEnd('/');
                    if (idxFolder.StartsWith(relativeFolder) && idxFolder.LastIndexOf("/") == relativeFolder.LastIndexOf("/"))
                        result.Add(_rootResolver.DynamicFiles.TrimEnd('/') + idxFolder + "/");
                }
                return result;
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
            var relPath = CqlFileService.BreakDownPath(path);
            var cql = "select folder from files where cloudlet = :cloudlet and folder = :folder and filename = ''";
            var args = new Dictionary<string, object>
            {
                { "cloudlet", rootResolver.DynamicFiles },
                { "folder", relPath.Folder },
            };
            var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
            var row = rs.FirstOrDefault();
            return row == null ? false : true;
        }

        #endregion
    }
}