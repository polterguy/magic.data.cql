/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.IO;
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

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        public ScyllaFileService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <inheritdoc />
        public void Copy(string source, string destination)
        {
            CopyAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task CopyAsync(string source, string destination)
        {
            using (var session = CreateSession())
            {
                var cql = "select content from files where filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "filename", source },
                };
                string content;
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault() ?? throw new HyperlambdaException("No such file");
                    content = row.GetValue<string>("content");
                }
                await SaveAsync(destination, content);
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
            using (var session = CreateSession())
            {
                var cql = "delete from files where filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "filename", path },
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
            using (var session = CreateSession())
            {
                var cql = "select filename from files where filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "filename", path },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault();
                    return row == null ? false : true;
                }
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
            using (var session = CreateSession())
            {
                var cql = "select filename from files where folder = :folder";
                var args = new Dictionary<string, object>
                {
                    { "folder", folder },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var result = new List<string>();
                    foreach (var idx in rs.GetRows())
                    {
                        var idxFile = idx.GetValue<string>("filename");
                        if (extension == null || idxFile.EndsWith(extension))
                            result.Add(idxFile);
                    }
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
            using (var session = CreateSession())
            {
                var cql = "select content from files where filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "filename", Path.GetFileName(path) },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault() ?? throw new HyperlambdaException("No such file found");
                    return row.GetValue<string>("content");
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
            using (var session = CreateSession())
            {
                var cql = "update files set folder = :dest where filename = :filename";
                var args = new Dictionary<string, object>
                {
                    { "filename", source },
                    { "dest", Path.GetDirectoryName(destination) },
                };
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
            using (var session = CreateSession())
            {
                var cql = "insert into files (filename, folder, content) values (:filename, :folder, :content)";
                var args = new Dictionary<string, object>
                {
                    { "filename", path },
                    { "folder", Path.GetDirectoryName(path) },
                    { "content", content }
                };
                await session.ExecuteAsync(new SimpleStatement(args, cql));
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

        #endregion
    }
}