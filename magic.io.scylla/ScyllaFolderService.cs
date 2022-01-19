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

namespace magic.io.scylla
{
    public class ScyllaFolderService : IFolderService
    {
        readonly IConfiguration _configuration;
        readonly IRootResolver _rootResolver;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        public ScyllaFolderService(IConfiguration configuration, IRootResolver rootResolver)
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
            var keys = ScyllaFileService.GetCloudletInstance(_rootResolver);
            using (var session = ScyllaFileService.CreateSession(_configuration))
            {
                var cql = "insert into folders (client, cloudlet, folder) values (:client, :cloudlet, :folder)";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
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
            var keys = ScyllaFileService.GetCloudletInstance(_rootResolver);
            var relativePath = _rootResolver.RelativePath(path);
            using (var session = ScyllaFileService.CreateSession(_configuration))
            {
                // Deleting main folder and all sub-folders.
                var cql = "select folder from folders where client = :client and cloudlet = :cloudlet";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                foreach (var idx in rs)
                {
                    var idxFolder = idx.GetValue<string>("folder");
                    if (idxFolder.StartsWith(relativePath))
                    {
                        cql = "delete from folders where client = :client and cloudlet = :cloudlet and folder = :folder";
                        args = new Dictionary<string, object>
                        {
                            { "client", keys.Client },
                            { "cloudlet", keys.Cloudlet },
                            { "folder", idxFolder },
                        };
                        await session.ExecuteAsync(new SimpleStatement(args, cql));
                    }
                }

                // Deleting all files within folder.
                cql = "select filename from files where client = :client and cloudlet = :cloudlet";
                args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                };
                rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                foreach (var idx in rs)
                {
                    var idxFile = idx.GetValue<string>("filename");
                    if (idxFile.StartsWith(relativePath))
                    {
                        cql = "delete from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                        args = new Dictionary<string, object>
                        {
                            { "client", keys.Client },
                            { "cloudlet", keys.Cloudlet },
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
            var keys = ScyllaFileService.GetCloudletInstance(_rootResolver);
            using (var session = ScyllaFileService.CreateSession(_configuration))
            {
                var cql = "select folder from folders where client = :client and cloudlet = :cloudlet and folder = :folder";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
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
            var keys = ScyllaFileService.GetCloudletInstance(_rootResolver);
            var relativeFolder = _rootResolver.RelativePath(folder).TrimEnd('/');
            using (var session = ScyllaFileService.CreateSession(_configuration))
            {
                var cql = "select folder from folders where client = :client and cloudlet = :cloudlet";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var result = new List<string>();
                foreach (var idx in rs.GetRows())
                {
                    var idxFolder = idx.GetValue<string>("folder").TrimEnd('/');
                    if (idxFolder.StartsWith(relativeFolder) && idxFolder.LastIndexOf("/") == relativeFolder.LastIndexOf("/"))
                        result.Add(idxFolder + "/");
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
            string client,
            string cloudlet,
            string relativePath)
        {
            var cql = "select folder from folders where client = :client and cloudlet = :cloudlet and folder = :folder";
            var args = new Dictionary<string, object>
            {
                { "client", client },
                { "cloudlet", cloudlet },
                { "folder", relativePath },
            };
            var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
            var row = rs.FirstOrDefault();
            return row == null ? false : true;
        }

        #endregion
    }
}