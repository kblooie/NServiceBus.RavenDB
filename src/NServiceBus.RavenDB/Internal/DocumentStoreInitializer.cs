namespace NServiceBus.Persistence.RavenDB
{
    using System;
    using NServiceBus.ConsistencyGuarantees;
    using NServiceBus.Settings;
    using Raven.Client.Documents;

    class DocumentStoreInitializer
    {
        internal DocumentStoreInitializer(Func<ReadOnlySettings, IDocumentStore> storeCreator)
        {
            this.storeCreator = storeCreator;
        }

        internal DocumentStoreInitializer(IDocumentStore store)
        {
            storeCreator = readOnlySettings => store;
        }

        public string Identifier => docStore?.Identifier;

        internal IDocumentStore Init(ReadOnlySettings settings)
        {
            if (!isInitialized)
            {
                EnsureDocStoreCreated(settings);
                ApplyConventions(settings);

                docStore.Initialize();
            }
            isInitialized = true;
            return docStore;
        }

        void EnsureDocStoreCreated(ReadOnlySettings settings)
        {
            if (docStore == null)
            {
                docStore = storeCreator(settings);
            }
        }

        void ApplyConventions(ReadOnlySettings settings)
        {
            var store = docStore as DocumentStore;
            if (store == null)
            {
                return;
            }

            UnwrappedSagaListener.Register(store);

            var isSendOnly = settings.GetOrDefault<bool>("Endpoint.SendOnly");
            if (!isSendOnly)
            {
                var usingDtc = settings.GetRequiredTransactionModeForReceives() == TransportTransactionMode.TransactionScope;
                if (usingDtc)
                {
                    throw new Exception("RavenDB does not support Distributed Transaction Coordinator (DTC) transactions. You must change the TransportTransactionMode in order to continue. See the RavenDB Persistence documentation for more details.");
                }
            }
        }

        Func<ReadOnlySettings, IDocumentStore> storeCreator;
        IDocumentStore docStore;
        bool isInitialized;
    }
}
