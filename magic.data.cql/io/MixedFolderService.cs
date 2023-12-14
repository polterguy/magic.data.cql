/*
 * Magic Cloud, copyright (c) 2023 Thomas Hansen. See the attached LICENSE file for details. For license inquiries you can send an email to thomas@ainiro.io
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using magic.node.services;
using magic.node.contracts;
using magic.node.extensions;

namespace magic.data.cql.io
{
    /// <summary>
    /// Folder service for Magic storing folders in ScyllaDB or the local file system, depending
    /// upon the initial path specified.
    /// </summary>
    public class MixedFolderService : IFolderService
    {
        readonly IRootResolver _rootResolver;
        readonly CqlFolderService _cqlFolderService;
        readonly FolderService _folderService;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        /// <param name="cqlFolderService">Needed to load folders from NoSQL storage.</param>
        /// <param name="folderService">Needed to load folders from local file system.</param>
        public MixedFolderService(
            IRootResolver rootResolver,
            CqlFolderService cqlFolderService,
            FolderService folderService)
        {
            _rootResolver = rootResolver;
            _cqlFolderService = cqlFolderService;
            _folderService = folderService;
        }

        /// <inheritdoc />
        public void Copy(string source, string destination)
        {
            CopyAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public Task CopyAsync(string source, string destination)
        {
            throw new HyperlambdaException("Move is not implemented for current folder service");
        }

        /// <inheritdoc />
        public void Create(string path)
        {
            GetImplementation(path, true).Create(path);
        }

        /// <inheritdoc />
        public Task CreateAsync(string path)
        {
            return GetImplementation(path, true).CreateAsync(path);
        }

        /// <inheritdoc />
        public void Delete(string path)
        {
            GetImplementation(path, true).Delete(path);
        }

        /// <inheritdoc />
        public Task DeleteAsync(string path)
        {
            return GetImplementation(path, true).DeleteAsync(path);
        }

        /// <inheritdoc />
        public bool Exists(string path)
        {
            return GetImplementation(path, false).Exists(path);
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync(string path)
        {
            return GetImplementation(path, false).ExistsAsync(path);
        }

        /// <inheritdoc />
        public List<string> ListFolders(string folder)
        {
            return GetImplementation(folder, false).ListFolders(folder);
        }

        /// <inheritdoc />
        public Task<List<string>> ListFoldersAsync(string folder)
        {
            return GetImplementation(folder, false).ListFoldersAsync(folder);
        }

        /// <inheritdoc />
        public List<string> ListFoldersRecursively(string folder)
        {
            return ListFoldersAsync(folder).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFoldersRecursivelyAsync(string folder)
        {
            if (folder == "/")
            {
                // Fetching files from both folders.
                var sysFiles = await _folderService.ListFoldersRecursivelyAsync(folder);
                var files = await _cqlFolderService.ListFoldersRecursivelyAsync(folder);
                files.AddRange(sysFiles);
                files.Sort();
                return files;
            }
            switch (folder.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).First())
            {
                case "system":
                case "misc":
                    // System files are requested.
                    return await _folderService.ListFoldersRecursivelyAsync(folder);

                default:
                    // User files are requested.
                    return await _cqlFolderService.ListFoldersRecursivelyAsync(folder);
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
            throw new HyperlambdaException("Move is not implemented for current folder service");
        }

        #region [ -- Private helper methods -- ]

        /*
         * Returns the correct implementation depending upon the path specified.
         */
        IFolderService GetImplementation(string destinationPath, bool change)
        {
            var relPath = 
                destinationPath.StartsWith(_rootResolver.DynamicFiles) ? 
                _rootResolver.RelativePath(destinationPath) : 
                destinationPath.Substring(_rootResolver.RootFolder.Length - 1);
            var splits = relPath.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (splits.Length > 1)
            {
                switch (splits.First())
                {
                    // System files, using local file storage service.
                    case "modules":
                    case "etc":
                        if (change)
                            throw new HyperlambdaException($"The file '{destinationPath}' is read only with your current file service implementation");
                        return _cqlFolderService;
                }
            }
            return _folderService;
        }

        #endregion
    }
}