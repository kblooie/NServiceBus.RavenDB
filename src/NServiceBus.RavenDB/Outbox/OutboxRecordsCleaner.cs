namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using NServiceBus.RavenDB.Outbox;
    using Raven.Client.Documents;
    using Raven.Client.Documents.Operations.CompareExchange;

    class OutboxRecordsCleaner
    {
        public OutboxRecordsCleaner(IDocumentStore documentStore, string endpointName)
        {
            this.documentStore = documentStore;
            this.endpointName = endpointName;
        }

        public async Task RemoveEntriesOlderThan(DateTime dateTime, CancellationToken cancellationToken = default)
        {
            var skip = 0;
            const int pageSize = 1024;

            do
            {
                var records = await documentStore.Operations
                    .SendAsync(new GetCompareExchangeValuesOperation<OutboxRecord>($"Outbox/{endpointName}/", skip, pageSize), token: cancellationToken).ConfigureAwait(false);
                
                foreach (var recordExchangeValue in records.Values)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var outboxRecord = recordExchangeValue.Value;
                    if (outboxRecord.Dispatched && outboxRecord.DispatchedAt <= dateTime)
                    {
                        //We won't check to see if the result is successful, as this is simply a cleanup routine
                        await documentStore.Operations
                            .SendAsync(new DeleteCompareExchangeValueOperation<OutboxRecord>(recordExchangeValue.Key, recordExchangeValue.Index)).ConfigureAwait(false);
                    }
                }

                if (records.Count < pageSize)
                    break;

                skip += records.Count;
            } while (true);
        }

        IDocumentStore documentStore;
        readonly string endpointName;
    }
}