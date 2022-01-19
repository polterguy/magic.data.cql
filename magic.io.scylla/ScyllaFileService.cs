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
using magic.node.extensions;

namespace magic.io.scylla
{
    public class ScyllaFileService : IFileService
    {
        readonly IConfiguration _configuration;
        readonly IRootResolver _rootResolver;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        public ScyllaFileService(IConfiguration configuration, IRootResolver rootResolver)
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                var cql = "select content from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                    { "filename", _rootResolver.RelativePath(source) },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var row = rs.FirstOrDefault() ?? throw new HyperlambdaException("No such file");
                var content = row.GetValue<string>("content");
                await SaveAsync(
                    session,
                    keys.Client,
                    keys.Cloudlet,
                    _rootResolver.RelativePath(destination),
                    content);
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                var cql = "delete from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                    { "filename", _rootResolver.RelativePath(path) },
                };
                await session.ExecuteAsync(new SimpleStatement(args, cql));
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                var cql = "select filename from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                    { "filename", _rootResolver.RelativePath(path) },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var row = rs.FirstOrDefault();
                return row == null ? false : true;
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
            var keys = GetCloudletInstance();
            var relativeFolder = _rootResolver.RelativePath(folder);
            using (var session = CreateSession())
            {
                var cql = "select filename from files where client = :client and cloudlet = :cloudlet";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var result = new List<string>();
                foreach (var idx in rs.GetRows())
                {
                    var idxFile = idx.GetValue<string>("filename");
                    if (extension == null || idxFile.EndsWith(extension))
                    {
                        if (idxFile.StartsWith(relativeFolder) && idxFile.LastIndexOf("/") == relativeFolder.LastIndexOf("/"))
                            result.Add(idxFile);
                    }
                }
                return result;
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                var cql = "select content from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                    { "filename", _rootResolver.RelativePath(path) },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var row = rs.FirstOrDefault() ?? throw new HyperlambdaException("No such file found");
                return row.GetValue<string>("content");
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                var cql = "select content from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "client", keys.Client },
                    { "cloudlet", keys.Cloudlet },
                    { "filename", _rootResolver.RelativePath(source) },
                };
                var rs = await session.ExecuteAsync(new SimpleStatement(args, cql));
                var row = rs.FirstOrDefault() ?? throw new HyperlambdaException("No such source file");
                var content = row.GetValue<string>("content");
                await SaveAsync(
                    session,
                    keys.Client,
                    keys.Cloudlet,
                    _rootResolver.RelativePath(destination),
                    content);
                cql = "delete from files where client = :client and cloudlet = :cloudlet and filename = :filename";
                await session.ExecuteAsync(new SimpleStatement(args, cql));
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
            var keys = GetCloudletInstance();
            using (var session = CreateSession())
            {
                await SaveAsync(
                    session,
                    keys.Client,
                    keys.Cloudlet,
                    _rootResolver.RelativePath(path),
                    content);
            }
        }

        /// <inheritdoc />
        public async Task SaveAsync(string path, byte[] content)
        {
            await SaveAsync(path, Convert.ToBase64String(content));
        }

        #region [ -- Private helper methods -- ]

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        ISession CreateSession()
        {
            var cluster = Cluster.Builder()
                .AddContactPoints(_configuration["magic:io:scylla:host"] ?? "127.0.0.1")
                .Build();
            return cluster.Connect("magic");
        }

        /*
         * Returns cloudlet and client ID using the root resolver.
         */
        (string Client, string Cloudlet) GetCloudletInstance()
        {
            var rootFolder = _rootResolver.RootFolder;
            var items = rootFolder.Split('/');
            var client = items.First();
            var cloudlet = string.Join("/", items.Skip(1));
            return (client, cloudlet);
        }

        /*
         * Common helper method to save file on specified session given specified client and cloudlet ID.
         */
        async Task SaveAsync(
            ISession session,
            string client,
            string cloudlet,
            string path,
            string content)
        {
            var cql = "insert into files (client, cloudlet, filename, content) values (:client, :cloudlet, :filename, :content)";
            var args = new Dictionary<string, object>
            {
                { "client", client },
                { "cloudlet", cloudlet },
                { "filename", path },
                { "content", content },
            };
            await session.ExecuteAsync(new SimpleStatement(args, cql));
        }

        #endregion
    }
}