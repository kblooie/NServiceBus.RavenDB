namespace NServiceBus.Persistence.RavenDB
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Raven.Client.Documents.Session;

    interface IOpenRavenSessionsInPipeline
    {
        Task<IAsyncDocumentSession> OpenSession(IDictionary<string, string> messageHeaders);
    }
}