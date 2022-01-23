/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.IO;
using System.Text;
using System.Threading.Tasks;
using magic.node.contracts;

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
            var result = new MemoryStream(await _fileService.LoadBinaryAsync(path));
            result.Position = 0;
            return result;
        }

        /// <inheritdoc />
        public void SaveFile(Stream stream, string path, bool overwrite)
        {
            SaveFileAsync(stream, path, overwrite).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public async Task SaveFileAsync(Stream stream, string path, bool overwrite)
        {
            var reader = new StreamReader(stream);
            var content = await reader.ReadToEndAsync();
            if (await _fileService.ExistsAsync(path))
                await _fileService.DeleteAsync(path);
            await _fileService.SaveAsync(path, content);
        }
    }
}