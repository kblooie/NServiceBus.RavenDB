﻿using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.Settings;
using System;
using System.Threading.Tasks;
using NServiceBus.Configuration.AdvancedExtensibility;
using Raven.Client.Documents;

public class ConfigureEndpointRavenDBPersistence : IConfigureEndpointTestExecution
{
    const string DefaultDocumentStoreKey = "$.ConfigureEndpointRavenDBPersistence.DefaultDocumentStore";
    const string DefaultPersistenceExtensionsKey = "$.ConfigureRavenDBPersistence.DefaultPersistenceExtensions";

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var documentStore = GetDocumentStore();

        databaseName = documentStore.DefaultDatabase;

        configuration.GetSettings().Set(DefaultDocumentStoreKey, documentStore);

        var persistenceExtensions = configuration.UsePersistence<RavenDBPersistence>()
            .DoNotSetupDatabasePermissions()
            .SetDefaultDocumentStore(documentStore)
            .DisableSubscriptionVersioning();

        configuration.GetSettings().Set(DefaultPersistenceExtensionsKey, persistenceExtensions);

        Console.WriteLine("Created '{0}' database", documentStore.DefaultDatabase);

        return Task.FromResult(0);
    }

    public Task Cleanup()
    {
        return DeleteDatabase(databaseName);
    }

    public static DocumentStore GetDocumentStore()
    {
        var dbName = Guid.NewGuid().ToString();

        var documentStore = GetInitializedDocumentStore(dbName);

        return documentStore;
    }

    internal static DocumentStore GetInitializedDocumentStore(string defaultDatabase)
    {
        var ravenUrl = Environment.GetEnvironmentVariable("RavenDbUrl") ?? "http://localhost:8084";
        var apiKey = Environment.GetEnvironmentVariable("RavenDbApiKey");
        var documentStore = new DocumentStore
        {
            Url = ravenUrl,
            DefaultDatabase = defaultDatabase,
            ApiKey = apiKey,
#if NET452
            EnlistInDistributedTransactions = false
#endif
        };

        documentStore.Initialize();

        return documentStore;
    }

    public static async Task DeleteDatabase(string dbName)
    {
        // Periodically the delete will throw an exception because Raven has the database locked
        // To solve this we have a retry loop with a delay
        var triesLeft = 3;

        while (triesLeft-- > 0)
        {
            try
            {
                // We are using a new store because the global one is disposed of before cleanup
                using (var storeForDeletion = GetInitializedDocumentStore(dbName))
                {
                    await storeForDeletion.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync(dbName, true);
                    break;
                }
            }
            catch
            {
                if (triesLeft == 0)
                {
                    throw;
                }

                await Task.Delay(250);
            }
        }

        Console.WriteLine("Deleted '{0}' database", dbName);
    }

    string databaseName;

    public static DocumentStore GetDefaultDocumentStore(ReadOnlySettings settings)
    {
        return settings.Get<DocumentStore>(DefaultDocumentStoreKey);
    }

    public static PersistenceExtensions<RavenDBPersistence> GetDefaultPersistenceExtensions(ReadOnlySettings settings)
    {
        return settings.Get<PersistenceExtensions<RavenDBPersistence>>(DefaultPersistenceExtensionsKey);
    }
}

public static class TestConfigurationExtensions
{
    public static PersistenceExtensions<RavenDBPersistence> ResetDocumentStoreSettings(this PersistenceExtensions<RavenDBPersistence> cfg, out TestDatabaseInfo dbInfo)
    {
        var settings = cfg.GetSettings();
        var docStore = ConfigureEndpointRavenDBPersistence.GetDefaultDocumentStore(settings);

        settings.Set("RavenDbDocumentStore", null);
        dbInfo = new TestDatabaseInfo
        {
            Url = docStore.Url,
            DatabaseName = docStore.DefaultDatabase
        };
        return cfg;
    }
}

public class TestDatabaseInfo
{
    public string Url { get; set; }
    public string DatabaseName { get; set; }
}
