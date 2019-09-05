namespace NServiceBus.RavenDB.Tests.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.Outbox;
    using NServiceBus.Persistence.RavenDB;
    using NServiceBus.RavenDB.Outbox;
    using NUnit.Framework;
    using Raven.Client.Documents;
    using Raven.Client.Documents.Operations.CompareExchange;

    [TestFixture]
    public class When_cleaning_outbox_messages : RavenDBPersistenceTestBase
    {
        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            new OutboxRecordsIndex().Execute(store);
        }

        [Test]
        public async Task Should_delete_all_OutboxRecords_that_have_been_dispatched()
        {
            var id = Guid.NewGuid().ToString("N");
            var context = new ContextBag();
            var endpointName = "TestEndpoint";

            var persister = new OutboxPersister(store, endpointName, CreateTestSessionOpener());

            using (var transaction = await persister.BeginTransaction(context))
            {
                await persister.Store(new OutboxMessage("NotDispatched", new TransportOperation[0]), transaction, context);

                await transaction.Commit();
            }

            var outboxMessage = new OutboxMessage(id, new []
                {
                    new TransportOperation(id, new Dictionary<string, string>(), new byte[1024*5], new Dictionary<string, string>())
                });


            using (var transaction = await persister.BeginTransaction(context))
            {
                await persister.Store(outboxMessage, transaction, context);
                await transaction.Commit();
            }


            await persister.SetAsDispatched(id, context);
            await Task.Delay(TimeSpan.FromSeconds(1)); //Need to wait for dispatch logic to finish
            
            var cleaner = new OutboxRecordsCleaner(store, endpointName);

            await cleaner.RemoveEntriesOlderThan(DateTime.UtcNow.AddMinutes(1));

            var result = (await store.Operations
                .SendAsync(new GetCompareExchangeValuesOperation<OutboxRecord>($"Outbox/{endpointName}/"))).Values;

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("NotDispatched", result.First().Value.MessageId);
        }
    }
}