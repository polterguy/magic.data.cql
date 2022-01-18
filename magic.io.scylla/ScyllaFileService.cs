/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Cassandra;
using magic.node.contracts;
using magic.node.extensions;

namespace magic.io.scylla
{
    public class ScyllaFileService : IFileService
    {
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
                var cql = "select content from files where path = :path";
                var args = new Dictionary<string, object>
                {
                    { "path", source },
                };
                string sourceFile;
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault();
                    sourceFile = row.GetValue<string>("content");
                }
                await SaveAsync(destination, sourceFile);
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
                var cql = "delete from files where path = :path";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
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
                var cql = "select path from files where path = :path";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
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
                var cql = "select path from files where folder = :folder";
                var args = new Dictionary<string, object>
                {
                    { "folder", folder },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var result = new List<string>();
                    foreach (var idx in rs.GetRows())
                    {
                        var value = idx.GetValue<string>("path");
                        if (extension == null || value.EndsWith(extension));
                            result.Add(value);
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
                var cql = "select content from files where path = :path";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault() ?? throw new HyperlambdaException($"No such file found, path was '{path}'");
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
            using (var session = CreateSession())
            {
                var cql = "select content from files where path = :path";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
                };
                using (var rs = await session.ExecuteAsync(new SimpleStatement(args, cql)))
                {
                    var row = rs.FirstOrDefault() ?? throw new HyperlambdaException($"No such file found, path was '{path}'");
                    return Convert.FromBase64String(row.GetValue<string>("content"));
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
            using (var session = CreateSession())
            {
                var cql = "update files set path = :dest where path = :src";
                var args = new Dictionary<string, object>
                {
                    { "src", source },
                    { "dest", destination },
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
                var cql = "insert into files (path, content) values (:path, :content)";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
                    { "content", content }
                };
                await session.ExecuteAsync(new SimpleStatement(args, cql));
            }
        }

        /// <inheritdoc />
        public async Task SaveAsync(string path, byte[] content)
        {
            using (var session = CreateSession())
            {
                var cql = "insert into files (path, content) values (:path, :content)";
                var args = new Dictionary<string, object>
                {
                    { "path", path },
                    { "content", Convert.ToBase64String(content) }
                };
                await session.ExecuteAsync(new SimpleStatement(args, cql));
            }
        }

        #region [ -- Private helper methods -- ]

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        ISession CreateSession()
        {
            var cluster = Cluster.Builder()
                .AddContactPoints("127.0.0.1")
                .Build();
            return cluster.Connect("magic");
        }

        #endregion
    }
}