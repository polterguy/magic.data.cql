/*
 * Magic Cloud, copyright Aista, Ltd and Thomas Hansen. See the attached LICENSE file for details. For license inquiries you can send an email to thomas@ainiro.io
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using magic.node.services;
using magic.node.contracts;
using magic.node.extensions;

namespace magic.data.cql.io
{
    /// <summary>
    /// File service for Magic storing files in ScyllaDB.
    /// </summary>
    public class MixedStreamService : IStreamService
    {
        readonly IRootResolver _rootResolver;
        readonly CqlStreamService _cqlStreamService;
        readonly StreamService _streamService;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="rootResolver">Needed to resolve paths.</param>
        /// <param name="cqlStreamService">Underlaying NoSQL file service.</param>
        /// <param name="streamService">Underlaying file system file service.</param>
        public MixedStreamService(
            IRootResolver rootResolver,
            CqlStreamService cqlStreamService,
            StreamService streamService)
        {
            _rootResolver = rootResolver;
            _cqlStreamService = cqlStreamService;
            _streamService = streamService;
        }

        /// <inheritdoc />
        public Stream OpenFile(string path)
        {
            return GetImplementation(path, false).OpenFile(path);
        }

        /// <inheritdoc />
        public Task<Stream> OpenFileAsync(string path)
        {
            return GetImplementation(path, false).OpenFileAsync(path);
        }

        /// <inheritdoc />
        public void SaveFile(Stream stream, string path, bool overwrite)
        {
            GetImplementation(path, true).SaveFile(stream, path, overwrite);
        }

        /// <inheritdoc />
        public Task SaveFileAsync(Stream stream, string path, bool overwrite)
        {
            return GetImplementation(path, true).SaveFileAsync(stream, path, overwrite);
        }

        #region [ -- Private helper methods -- ]

        /*
         * Returns the correct implementation depending upon the path specified.
         */
        IStreamService GetImplementation(string destinationPath, bool change)
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
                            throw new HyperlambdaException($"The file '{destinationPath}' is read only with your current stream service implementation");
                        return _cqlStreamService;
                }
            }
            return _streamService;
        }

        #endregion
    }
}
