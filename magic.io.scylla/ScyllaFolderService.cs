/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
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

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration needed to retrieve connection settings to ScyllaDB.</param>
        public ScyllaFolderService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Copy(string source, string destination)
        {
            throw new NotImplementedException();
        }

        public Task CopyAsync(string source, string destination)
        {
            throw new NotImplementedException();
        }

        public void Create(string path)
        {
            throw new NotImplementedException();
        }

        public Task CreateAsync(string path)
        {
            throw new NotImplementedException();
        }

        public void Delete(string path)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(string path)
        {
            throw new NotImplementedException();
        }

        public bool Exists(string path)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ExistsAsync(string path)
        {
            throw new NotImplementedException();
        }

        public List<string> ListFolders(string folder)
        {
            throw new NotImplementedException();
        }

        public Task<List<string>> ListFoldersAsync(string folder)
        {
            throw new NotImplementedException();
        }

        public void Move(string source, string destination)
        {
            throw new NotImplementedException();
        }

        public Task MoveAsync(string source, string destination)
        {
            throw new NotImplementedException();
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