using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.RavenDB.Persistence.SagaPersister;
using NServiceBus.RavenDB.Tests;
using NServiceBus.SagaPersisters.RavenDB;
using NUnit.Framework;
using Raven.Client;

[TestFixture]
public class When_completing_a_saga_with_unique_property : RavenDBPersistenceTestBase
{
    [Test]
    public async Task Should_delete_the_saga_and_the_unique_doc()
    {
        var sagaId = Guid.NewGuid();

        IAsyncDocumentSession session;
        var options = this.CreateContextWithAsyncSessionPresent(out session);
        var persister = new SagaPersister();
        var entity = new SagaData
        {
            Id = sagaId
        };
        var synchronizedSession = new RavenDBSynchronizedStorageSession(session, true);

        await persister.Save(entity, this.CreateMetadata<SomeSaga>(entity), synchronizedSession, options);
        await session.SaveChangesAsync().ConfigureAwait(false);

        var saga = await persister.Get<SagaData>(sagaId, synchronizedSession, options);
        await persister.Complete(saga, synchronizedSession, options);
        await session.SaveChangesAsync().ConfigureAwait(false);

        Assert.Null(await persister.Get<SagaData>(sagaId, synchronizedSession, options));
        Assert.Null(await session.Query<SagaUniqueIdentity>().Customize(c => c.WaitForNonStaleResults()).SingleOrDefaultAsync(u => u.SagaId == sagaId).ConfigureAwait(false));
    }

    class SomeSaga : Saga<SagaData>, IAmStartedByMessages<StartMessage>
    {
        public Task Handle(StartMessage message, IMessageHandlerContext context)
        {
            return Task.FromResult(0);
        }

        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SagaData> mapper)
        {
            mapper.ConfigureMapping<StartMessage>(m => m.SomeId).ToSaga(s => s.SomeId);
        }
    }

    class StartMessage : IMessage
    {
        public Guid SomeId { get; set; }
    }

    class SagaData : IContainSagaData
    {
        public Guid SomeId { get; set; }
        public Guid Id { get; set; }
        public string Originator { get; set; }
        public string OriginalMessageId { get; set; }
    }
}