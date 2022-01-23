/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.IO;
using System.Threading.Tasks;
using magic.node.contracts;
using magic.node.extensions;

namespace magic.data.cql.io
{
    /// <summary>
    /// File service for Magic storing files in ScyllaDB.
    /// </summary>
    public class CqlStreamService : IStreamService
    {
        readonly IFileService _fileService;

        /// <summary>
        /// Creates an instance of your type.
        /// </summary>
        /// <param name="fileService">Underlaying file service.</param>
        public CqlStreamService(IFileService fileService)
        {
            _fileService = fileService;
        }

        /// <inheritdoc />
        public Stream OpenFile(string path)
        {
            return OpenFileAsync(path).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task<Stream> OpenFileAsync(string path)
        {
            return new MemoryStream(await _fileService.LoadBinaryAsync(path));
        }

        /// <inheritdoc />
        public void SaveFile(Stream stream, string path, bool overwrite)
        {
            SaveFileAsync(stream, path, overwrite).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task SaveFileAsync(Stream stream, string path, bool overwrite)
        {
            using (var memory = new MemoryStream())
            {
                await stream.CopyToAsync(memory);
                memory.Position = 0;
                if (await _fileService.ExistsAsync(path))
                {
                    if (overwrite)
                        await _fileService.DeleteAsync(path);
                    else
                        throw new HyperlambdaException("File already exists and overwrite argument was false");
                }
                await _fileService.SaveAsync(path, memory.ToArray());
            }
        }
    }
}