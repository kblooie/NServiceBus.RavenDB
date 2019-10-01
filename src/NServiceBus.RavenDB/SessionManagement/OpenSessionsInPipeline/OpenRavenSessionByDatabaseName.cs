namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Raven.Client.Documents.Session;

    class OpenRavenSessionByDatabaseName : IOpenRavenSessionsInPipeline
    {
        IDocumentStoreWrapper documentStoreWrapper;
        Func<IDictionary<string, string>, Task<string>> getDatabaseName;

        public OpenRavenSessionByDatabaseName(IDocumentStoreWrapper documentStoreWrapper, Func<IDictionary<string, string>, Task<string>> getDatabaseName = null)
        {
            this.documentStoreWrapper = documentStoreWrapper;
            this.getDatabaseName = getDatabaseName ?? (context => Task.FromResult(string.Empty));
        }

        public async Task<IAsyncDocumentSession> OpenSession(IDictionary<string, string> messageHeaders)
        {
            var databaseName = await getDatabaseName(messageHeaders).ConfigureAwait(false);
            var documentSession = string.IsNullOrEmpty(databaseName)
                ? documentStoreWrapper.DocumentStore.OpenAsyncSession()
                : documentStoreWrapper.DocumentStore.OpenAsyncSession(databaseName);

            documentSession.Advanced.UseOptimisticConcurrency = true;

            return documentSession;
        }
    }
}