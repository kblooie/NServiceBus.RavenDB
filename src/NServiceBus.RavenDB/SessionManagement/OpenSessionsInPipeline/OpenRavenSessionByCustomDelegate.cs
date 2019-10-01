namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Raven.Client.Documents.Session;

    class OpenRavenSessionByCustomDelegate : IOpenRavenSessionsInPipeline
    {
        Func<IDictionary<string, string>, Task<IAsyncDocumentSession>> getAsyncSessionUsingHeaders;

        public OpenRavenSessionByCustomDelegate(Func<IDictionary<string, string>, Task<IAsyncDocumentSession>> getAsyncSession)
        {
            this.getAsyncSessionUsingHeaders = getAsyncSession;
        }

        public async Task<IAsyncDocumentSession> OpenSession(IDictionary<string, string> messageHeaders)
        {
            var session = await getAsyncSessionUsingHeaders(messageHeaders).ConfigureAwait(false);

            session.Advanced.UseOptimisticConcurrency = true;

            return session;
        }
    }
}