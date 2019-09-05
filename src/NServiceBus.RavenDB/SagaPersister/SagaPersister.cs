namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.RavenDB.Persistence.SagaPersister;
    using NServiceBus.Sagas;
    using Raven.Client.Documents.Operations.CompareExchange;
    using Raven.Client.Documents.Session;

    class SagaPersister : ISagaPersister
    {
        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, SynchronizedStorageSession session, ContextBag context)
        {
            var documentSession = session.RavenSession();

            if (sagaData == null)
            {
                return Task.CompletedTask;
            }

            var docId = DocumentIdForSagaData(documentSession, sagaData);
            var identityDocId = correlationProperty != null
                ? SagaUniqueIdentity.FormatId(sagaData.GetType(), correlationProperty.Name, correlationProperty.Value)
                : null;

            documentSession.Advanced.ClusterTransaction.CreateCompareExchangeValue(docId, new SagaDataContainer
            {
                Id = docId,
                Data = sagaData,
                IdentityDocId = identityDocId
            });

            if (correlationProperty != null)
            {
                documentSession.Advanced.ClusterTransaction.CreateCompareExchangeValue(identityDocId, new SagaUniqueIdentity
                {
                    Id = identityDocId,
                    SagaId = sagaData.Id,
                    UniqueValue = correlationProperty.Value,
                    SagaDocId = docId
                });
            }

            return Task.CompletedTask;
        }

        public Task Update(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            var documentSession = session.RavenSession();
            var docId = DocumentIdForSagaData(documentSession, sagaData);

            var compareExchange = context.Get<CompareExchangeValue<SagaDataContainer>>($"{SagaCompareExchangeContextKeyPrefix}{docId}");
            compareExchange.Value.Data = sagaData;
            documentSession.Advanced.ClusterTransaction.UpdateCompareExchangeValue(compareExchange);

            return Task.CompletedTask;
        }

        public async Task<T> Get<T>(Guid sagaId, SynchronizedStorageSession session, ContextBag context)
            where T : class, IContainSagaData
        {
            var documentSession = session.RavenSession();
            var docId = DocumentIdForSagaData(documentSession, typeof(T), sagaId);

            var compX = await documentSession.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<SagaDataContainer>(docId).ConfigureAwait(false);

            var container = compX?.Value;

            if (container == null)
            {
                return default;
            }

            context.Set($"{SagaCompareExchangeContextKeyPrefix}{container.Data.Id}", compX);

            return container.Data as T;
        }

        public async Task<T> Get<T>(string propertyName, object propertyValue, SynchronizedStorageSession session, ContextBag context)
            where T : class, IContainSagaData
        {
            var documentSession = session.RavenSession();

            var lookupId = SagaUniqueIdentity.FormatId(typeof(T), propertyName, propertyValue);

            var lookup = await GetSagaUniqueIdentityCompareExchangeValue(documentSession, lookupId).ConfigureAwait(false);

            if (lookup == null) return default;

            return await Get<T>(lookup.Value.SagaId, session, context).ConfigureAwait(false);
        }

        public async Task Complete(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            var documentSession = session.RavenSession();

            var compareExchangeValue = context.Get<CompareExchangeValue<SagaDataContainer>>($"{SagaCompareExchangeContextKeyPrefix}{sagaData.Id}");
            documentSession.Advanced.ClusterTransaction.DeleteCompareExchangeValue(compareExchangeValue);

            var lookup = await GetSagaUniqueIdentityCompareExchangeValue(documentSession, compareExchangeValue.Value.IdentityDocId).ConfigureAwait(false);
            documentSession.Advanced.ClusterTransaction.DeleteCompareExchangeValue(lookup);
        }

        static string DocumentIdForSagaData(IAsyncDocumentSession documentSession, IContainSagaData sagaData)
        {
            return DocumentIdForSagaData(documentSession, sagaData.GetType(), sagaData.Id);
        }

        static string DocumentIdForSagaData(IAsyncDocumentSession documentSession, Type sagaDataType, Guid sagaId)
        {
            var conventions = documentSession.Advanced.DocumentStore.Conventions;
            var collectionName = conventions.FindCollectionName(sagaDataType);
            return $"{collectionName}{conventions.IdentityPartsSeparator}{sagaId}";
        }

        static async Task<CompareExchangeValue<SagaUniqueIdentity>> GetSagaUniqueIdentityCompareExchangeValue(IAsyncDocumentSession documentSession, string lookupId)
        {
            return await documentSession.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<SagaUniqueIdentity>(lookupId).ConfigureAwait(false);
        }

        const string SagaCompareExchangeContextKeyPrefix = "SagaCompareExchange:";
    }
}