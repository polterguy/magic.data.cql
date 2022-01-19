/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using magic.node;
using magic.data.cql.io;
using magic.node.extensions;
using magic.signals.contracts;

namespace magic.data.cql.slots
{
    /// <summary>
    /// [cql.connect] slot, for connecting to a CQL database instance,
    /// according to your configuration settings.
    /// </summary>
    [Slot(Name = "cql.connect")]
    public class Connect : ISlot, ISlotAsync
    {
        readonly IConfiguration _configuration;

        /// <summary>
        /// Creates a new instance of your type.
        /// </summary>
        /// <param name="configuration">Configuration for your application.</param>
        public Connect(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <summary>
        /// Implementation of your slot.
        /// </summary>
        /// <param name="signaler">Signaler used to raise the signal.</param>
        /// <param name="input">Arguments to your slot.</param>
        public void Signal(ISignaler signaler, Node input)
        {
            using (var session = CqlFileService.CreateSession(_configuration, input.GetEx<string>()))
            {
                signaler.Scope(
                    "cql.connect",
                    session,
                    () => signaler.Signal("eval", input));
                input.Value = null;
            }
        }

        /// <summary>
        /// Implementation of your slot.
        /// </summary>
        /// <param name="signaler">Signaler used to raise the signal.</param>
        /// <param name="input">Arguments to your slot.</param>
        /// <returns>An awaitable task.</returns>
        public async Task SignalAsync(ISignaler signaler, Node input)
        {
            using (var session = CqlFileService.CreateSession(_configuration, input.GetEx<string>()))
            {
                await signaler.ScopeAsync(
                    "cql.connect",
                    session,
                    async () => await signaler.SignalAsync("eval", input));
                input.Value = null;
            }
        }
    }
}
