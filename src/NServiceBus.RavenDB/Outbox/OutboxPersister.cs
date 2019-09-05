namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.Outbox;
    using NServiceBus.RavenDB.Outbox;
    using NServiceBus.Transport;
    using Raven.Client.Documents;
    using Raven.Client.Documents.Session;
    using TransportOperation = NServiceBus.Outbox.TransportOperation;

    class OutboxPersister : IOutboxStorage
    {
        public OutboxPersister(IDocumentStore documentStore, string endpointName, IOpenRavenSessionsInPipeline sessionCreator)
        {
            this.documentStore = documentStore;
            this.endpointName = endpointName;
            this.sessionCreator = sessionCreator;
        }

        public async Task<OutboxMessage> Get(string messageId, ContextBag options)
        {
            OutboxRecord result;
            using (var session = GetSession(options))
            {
                var outboxDocId = GetOutboxRecordId(messageId);
                var compX = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<OutboxRecord>(outboxDocId).ConfigureAwait(false);
                result = compX?.Value;
            }

            if (result == null)
            {
                return default;
            }

            if (result.Dispatched || result.TransportOperations.Length == 0)
            {
                return new OutboxMessage(result.MessageId, emptyTransportOperations);
            }

            var transportOperations = new TransportOperation[result.TransportOperations.Length];
            var index = 0;
            foreach (var op in result.TransportOperations)
            {
                transportOperations[index] = new TransportOperation(op.MessageId, op.Options, op.Message, op.Headers);
                index++;
            }

            return new OutboxMessage(result.MessageId, transportOperations);
        }


        public Task<OutboxTransaction> BeginTransaction(ContextBag context)
        {
            var session = GetSession(context);

            context.Set(session);
            var transaction = new RavenDBOutboxTransaction(session);
            return Task.FromResult<OutboxTransaction>(transaction);
        }

        public Task Store(OutboxMessage message, OutboxTransaction transaction, ContextBag context)
        {
            var session = ((RavenDBOutboxTransaction)transaction).AsyncSession;

            var operations = new OutboxRecord.OutboxOperation[message.TransportOperations.Length];

            var index = 0;
            foreach (var transportOperation in message.TransportOperations)
            {
                operations[index] = new OutboxRecord.OutboxOperation
                {
                    Message = transportOperation.Body,
                    Headers = transportOperation.Headers,
                    MessageId = transportOperation.MessageId,
                    Options = transportOperation.Options
                };
                index++;
            }
            
            var outboxDocId = GetOutboxRecordId(message.MessageId);
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(outboxDocId, new OutboxRecord
            {
                MessageId = message.MessageId,
                Dispatched = false,
                TransportOperations = operations
            });

            return Task.CompletedTask;
        }

        public async Task SetAsDispatched(string messageId, ContextBag options)
        {
            using (var session = GetSession(options))
            {
                var outboxDocId = GetOutboxRecordId(messageId);
                var compX = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<OutboxRecord>(outboxDocId).ConfigureAwait(false);
                
                var outboxMessage = compX?.Value;
                if (outboxMessage == null || outboxMessage.Dispatched)
                {
                    return;
                }

                outboxMessage.Dispatched = true;
                outboxMessage.DispatchedAt = DateTime.UtcNow;
                outboxMessage.TransportOperations = emptyOutboxOperations;

                session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(compX);

                await session.SaveChangesAsync().ConfigureAwait(false);
            }
        }

        IAsyncDocumentSession GetSession(ContextBag context)
        {
            var session = context.TryGet(out IncomingMessage message) 
                ? sessionCreator.OpenSession(message.Headers) 
                : documentStore.OpenAsyncSession();

            session.Advanced.SetTransactionMode(TransactionMode.ClusterWide);

            return session;
        }

        string GetOutboxRecordId(string messageId) => $"Outbox/{endpointName}/{messageId.Replace('\\', '_')}";

        string endpointName;
        IDocumentStore documentStore;
        TransportOperation[] emptyTransportOperations = new TransportOperation[0];
        OutboxRecord.OutboxOperation[] emptyOutboxOperations = new OutboxRecord.OutboxOperation[0];
        IOpenRavenSessionsInPipeline sessionCreator;
    }
}