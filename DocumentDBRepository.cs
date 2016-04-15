namespace NET.DocumentDb.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    namespace Azure.Documents.Db
    {
        using System.Configuration;
        using System.Linq;

        using Microsoft.Azure.Documents;
        using Microsoft.Azure.Documents.Client;
        using Microsoft.Azure.Documents.Linq;

        public class DocumentDbRepository
        {
            public DocumentDbRepository(string collection)
            {
                this.CollectionId = collection;
            }

            public IEnumerable<T> GetItems<T>(Expression<Func<T, bool>> predicate)
            {
                return this.Client.CreateDocumentQuery<T>(this.Collection.DocumentsLink, new FeedOptions { EnableScanInQuery = true })
                    .Where(predicate)
                    .AsEnumerable();
            }

            public IEnumerable<T> GetItems<T>(Expression<Func<T, bool>> predicate, FeedOptions options)
            {
                return this.Client.CreateDocumentQuery<T>(this.Collection.DocumentsLink, options)
                    .Where(predicate)
                    .AsEnumerable();
            }

            /// <summary>
            /// Get items by string expression query
            /// </summary>
            /// <typeparam name="T">Entity type to retorn</typeparam>
            /// <param name="expression">Query expression to get entities result</param>
            /// <returns>Entities returned by query expression</returns>
            public IEnumerable<T> GetItems<T>(string expression)
            {
                return this.Client.CreateDocumentQuery<T>(this.Collection.DocumentsLink, expression).AsEnumerable();
            }

            /// <summary>
            /// Get item by string expression query
            /// </summary>
            /// <typeparam name="T">Entity type to retorn</typeparam>
            /// <param name="expression">Query expression to get first or default entity</param>
            /// <returns>Entity returned by query expression</returns>
            public T GetItem<T>(string expression)
            {
                return this.Client.CreateDocumentQuery<T>(this.Collection.DocumentsLink, expression)
                            .AsEnumerable()
                            .FirstOrDefault();
            }

            public T GetItem<T>(Expression<Func<T, bool>> predicate)
            {
                return this.Client.CreateDocumentQuery<T>(this.Collection.DocumentsLink)
                            .Where(predicate)
                            .AsEnumerable()
                            .FirstOrDefault();
            }

            public async Task CreateItemAsync<T>(T item)
            {
                await this.Client.CreateDocumentAsync(this.Collection.SelfLink, item);
            }

            public async Task UpdateItemAsync<T>(string id, T item)
            {
                var doc = this.GetDocument(id);
                await this.Client.ReplaceDocumentAsync(doc.SelfLink, item);
            }

            //Use the Database if it exists, if not create a new Database
            private Database ReadOrCreateDatabase()
            {
                var db = this.Client.CreateDatabaseQuery()
                             .Where(d => d.Id == this.DatabaseId)
                             .AsEnumerable()
                             .FirstOrDefault() ?? this.Client.CreateDatabaseAsync(new Database { Id = this.DatabaseId }).Result;

                return db;
            }

            private Document GetDocument(string id)
            {
                return this.Client.CreateDocumentQuery(this.Collection.DocumentsLink)
                    .Where(d => d.Id == id)
                    .AsEnumerable()
                    .FirstOrDefault();
            }

            //Use the DocumentCollection if it exists, if not create a new Collection
            private DocumentCollection ReadOrCreateCollection(string databaseLink)
            {
                var col = this.Client.CreateDocumentCollectionQuery(databaseLink)
                                  .Where(c => c.Id == this.CollectionId)
                                  .AsEnumerable()
                                  .FirstOrDefault();

                if (col != null)
                {
                    return col;
                }

                var collectionSpec = new DocumentCollection { Id = this.CollectionId };
                var requestOptions = new RequestOptions { OfferType = "S1" };

                col = this.Client.CreateDocumentCollectionAsync(databaseLink, collectionSpec, requestOptions).Result;
                col.IndexingPolicy.IncludedPaths.Add(
                    new IncludedPath
                    {
                        Path = "/*",
                        Indexes = new Collection<Index>
                        {
                            new RangeIndex(DataType.Number) { Precision = 7 }
                        }
                    });

                return col;
            }

            //Expose the "database" value from configuration as a property for internal use
            private string databaseId;
            private string DatabaseId
            {
                get
                {
                    if (string.IsNullOrEmpty(this.databaseId))
                    {
                        this.databaseId = ConfigurationManager.AppSettings["database"];
                    }

                    return this.databaseId;
                }
            }

            //Expose the "collection" value from configuration as a property for internal use
            private string CollectionId { get; }

            //Use the ReadOrCreateDatabase function to get a reference to the database.
            private Database database;
            private Database Database => this.database ?? (this.database = this.ReadOrCreateDatabase());

            //Use the ReadOrCreateCollection function to get a reference to the collection.
            private DocumentCollection collection;
            private DocumentCollection Collection => this.collection ?? (this.collection = this.ReadOrCreateCollection(this.Database.SelfLink));

            //This property establishes a new connection to DocumentDB the first time it is used, 
            //and then reuses this instance for the duration of the application avoiding the
            //overhead of instantiating a new instance of DocumentClient with each request
            private DocumentClient client;
            private DocumentClient Client
            {
                get
                {
                    if (this.client != null)
                    {
                        return this.client;
                    }

                    var endpoint = ConfigurationManager.AppSettings["endpoint"];
                    var authKey = ConfigurationManager.AppSettings["authKey"];
                    var endpointUri = new Uri(endpoint);

                    this.client = new DocumentClient(endpointUri, authKey);
                    return this.client;
                }
            }
        }
    }
}
