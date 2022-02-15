/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
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
    /// File service for Magic storing files in ScyllaDB or the local file system, depending
    /// upon the initial path specified.
    /// </summary>
    public class MixedFileService : IFileService
    {
        readonly IRootResolver _rootResolver;
        readonly CqlFileService _cqlFileService;
        readonly FileService _fileService;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="rootResolver">Needed to resolve client and cloudlet.</param>
        /// <param name="cqlFileService">Needed to load files from NoSQL storage.</param>
        /// <param name="fileService">Needed to load files from local file system.</param>
        public MixedFileService(
            IRootResolver rootResolver,
            CqlFileService cqlFileService,
            FileService fileService)
        {
            _rootResolver = rootResolver;
            _cqlFileService= cqlFileService;
            _fileService = fileService;
        }

        /// <inheritdoc />
        public void Copy(string source, string destination)
        {
            CopyAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task CopyAsync(string source, string destination)
        {
            var srcService = GetImplementation(source, false);
            var destService = GetImplementation(destination, true);
            var content = await srcService.LoadBinaryAsync(source);
            await destService.SaveAsync(destination, content);
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
        public List<string> ListFiles(string folder, string extension = null)
        {
            return GetImplementation(folder, false).ListFiles(folder, extension);
        }

        /// <inheritdoc />
        public Task<List<string>> ListFilesAsync(string folder, string extension = null)
        {
            return GetImplementation(folder, false).ListFilesAsync(folder, extension);
        }

        /// <inheritdoc />
        public List<string> ListFilesRecursively(string folder, string extension = null)
        {
            return ListFilesRecursivelyAsync(folder, extension).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<List<string>> ListFilesRecursivelyAsync(string folder, string extension = null)
        {
            if (folder == "/")
            {
                // Fetching files from both folders.
                var sysFiles = await _fileService.ListFilesRecursivelyAsync(folder, extension);
                var files = await _cqlFileService.ListFilesRecursivelyAsync(folder, extension);
                files.AddRange(sysFiles);
                files.Sort();
                return files;
            }
            switch (folder.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).First())
            {
                case "system":
                case "misc":
                    // System files are requested.
                    return await _fileService.ListFilesRecursivelyAsync(folder, extension);

                default:
                    // User files are requested.
                    return await _cqlFileService.ListFilesRecursivelyAsync(folder, extension);
            }
        }

        /// <inheritdoc />
        public string Load(string path)
        {
            return GetImplementation(path, false).Load(path);
        }

        /// <inheritdoc />
        public Task<string> LoadAsync(string path)
        {
            return GetImplementation(path, false).LoadAsync(path);
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
            if (folder == "/")
            {
                // Fetching files from both folders.
                var sysFiles = await _fileService.LoadRecursivelyAsync(folder, extension);
                var files = (await _cqlFileService.LoadRecursivelyAsync(folder, extension)).ToList();
                files.AddRange(sysFiles);
                files.Sort((lhs, rhs) =>
                {
                    return lhs.Filename.CompareTo(rhs.Filename);
                });
                return files;
            }
            switch (folder.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).First())
            {
                case "system":
                case "misc":
                    // System files are requested.
                    return await _fileService.LoadRecursivelyAsync(folder, extension);

                default:
                    // User files are requested.
                    return await _cqlFileService.LoadRecursivelyAsync(folder, extension);
            }
        }

        /// <inheritdoc />
        public byte[] LoadBinary(string path)
        {
            return GetImplementation(path, false).LoadBinary(path);
        }

        /// <inheritdoc />
        public Task<byte[]> LoadBinaryAsync(string path)
        {
            return GetImplementation(path, false).LoadBinaryAsync(path);
        }

        /// <inheritdoc />
        public void Move(string source, string destination)
        {
            MoveAsync(source, destination).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task MoveAsync(string source, string destination)
        {
            var srcService = GetImplementation(source, true);
            var destService = GetImplementation(destination, true);
            var content = await srcService.LoadBinaryAsync(source);
            await destService.SaveAsync(destination, content);
            await srcService.DeleteAsync(source);
        }

        /// <inheritdoc />
        public void Save(string path, string content)
        {
            GetImplementation(path, true).Save(path, content);
        }

        /// <inheritdoc />
        public void Save(string path, byte[] content)
        {
            GetImplementation(path, true).Save(path, content);
        }

        /// <inheritdoc />
        public Task SaveAsync(string path, string content)
        {
            return GetImplementation(path, true).SaveAsync(path, content);
        }

        /// <inheritdoc />
        public Task SaveAsync(string path, byte[] content)
        {
            return GetImplementation(path, true).SaveAsync(path, content);
        }

        #region [ -- Internal helper methods -- ]

        /*
         * Returns the correct implementation depending upon the path specified.
         */
        IFileService GetImplementation(string destinationPath, bool change)
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
                        return _cqlFileService;
                }
            }
            return _fileService;
        }

        #endregion
    }
}
