﻿using System;
using System.Collections.Generic;
using System.Linq;
using Org.Infinispan.Query.Remote.Client;
using System.IO;
using Infinispan.Hotrod.Core.Tests.Util;
using Infinispan.Hotrod.Core;
using Xunit;
using Org.Infinispan.Protostream;
using SampleBankAccount;
using System.Text;
using System.Reflection;
/**
 * No queries use pagination as JPQL itself does not support it.
 * 
 */

namespace Infinispan.Hotrod.Core.XUnitTest
{
    public class RemoteQueryTestFixture : IDisposable
    {
        public HotRodServer hotRodServer { get; private set; }
        public const String NAMED_CACHE = "CacheForQueryTest";
        public const String ERRORS_KEY_SUFFIX = ".errors";
        public const String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
        public Cache<Object, Object> cache;
        public Cache<String, String> metaCache;
        public RemoteQueryTestFixture()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var arr = assembly.GetManifestResourceNames();
            Console.WriteLine("arr" + arr);
            var resourceName = "Infinispan.Hotrod.Core.XUnitTest.resources.proto2.bank.proto";
            string protoDef;
            using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            using (StreamReader reader = new StreamReader(stream))
            {
                protoDef = reader.ReadToEnd();
            }
            hotRodServer = new HotRodServer("infinispan.xml");
            hotRodServer.StartHotRodServer();
            var infinispan = new InfinispanDG();
            infinispan.AddHost("127.0.0.1");
            infinispan.Version = 0x1f;
            infinispan.ForceReturnValue = false;
            infinispan.ClientIntelligence = 0x01;

            metaCache = infinispan.newCache(new StringMarshaller(), new StringMarshaller(), PROTOBUF_METADATA_CACHE_NAME);
            // TODO: fix syntax below
            MediaType kvMediaType = new MediaType();
            kvMediaType.CustomMediaType = Encoding.ASCII.GetBytes("text/plain");
            kvMediaType.InfoType = 2;
            metaCache.KeyMediaType = kvMediaType;
            metaCache.ValueMediaType = kvMediaType;
            metaCache.Remove(ERRORS_KEY_SUFFIX).Wait();

            metaCache.Put("sample_bank_account/bank.proto", protoDef).Wait();
            if (metaCache.ContainsKey(ERRORS_KEY_SUFFIX).Result)
            {
                Assert.True(false, "fail: error in registering .proto model");
            }

            cache = infinispan.newCache(new BasicTypesProtoStreamMarshaller(), new BasicTypesProtoStreamMarshaller(), NAMED_CACHE);

            MediaType kvMT = new MediaType();
            kvMT.CustomMediaType = Encoding.ASCII.GetBytes("application/x-protostream");
            kvMT.InfoType = 2;
            cache.KeyMediaType = kvMT;
            cache.ValueMediaType = kvMT;

            cache.Clear().Wait();
            PutUsers(cache);
            PutAccounts(cache);
            PutTransactions(cache);

        }

        public void Dispose()
        {
            hotRodServer.Dispose();
        }
        private void PutUsers(Cache<Object, Object> remoteCache)
        {
            User user1 = new User();
            user1.Id = 1;
            user1.Name = "John";
            user1.Surname = "Doe";
            user1.Gender = User.Types.Gender.Male;
            user1.Age = 22;
            user1.Notes = "Lorem ipsum dolor sit amet";
            List<Int32> accountIds = new List<Int32>();
            accountIds.Add(1);
            accountIds.Add(2);
            user1.AccountIds.Add(accountIds);
            User.Types.Address address1 = new User.Types.Address();
            address1.Street = "Main Street";
            address1.PostCode = "X1234";
            address1.Number = 156;
            List<User.Types.Address> addresses = new List<User.Types.Address>();
            addresses.Add(address1);
            user1.Addresses.Add(addresses);

            _ = remoteCache.Put(1, user1);

            User user2 = new User();
            user2.Id = 2;
            user2.Name = "Spider";
            user2.Surname = "Man";
            user2.Gender = User.Types.Gender.Male;
            accountIds = new List<Int32>();
            accountIds.Add(3);
            user2.AccountIds.Add(accountIds);
            User.Types.Address address2 = new User.Types.Address();
            address2.Street = "Old Street";
            address2.PostCode = "Y12";
            address2.Number = -12;
            User.Types.Address address3 = new User.Types.Address();
            address3.Street = "Bond Street";
            address3.PostCode = "ZZ";
            address3.Number = 312;
            addresses = new List<User.Types.Address>();
            addresses.Add(address2);
            addresses.Add(address3);
            user2.Addresses.Add(addresses);

            _ = remoteCache.Put(2, user2);

            User user3 = new User();
            user3.Id = 3;
            user3.Name = "Spider";
            user3.Surname = "Woman";
            user3.Gender = User.Types.Gender.Female;

            _ = remoteCache.Put(3, user3);
            accountIds = new List<Int32>();
            user3.AccountIds.Add(accountIds);
        }

        private void PutAccounts(Cache<Object, Object> remoteCache)
        {
            Account account1 = new Account();
            account1.Id = 1;
            account1.Description = "John Doe's first bank account";
            account1.CreationDate = MakeDate("2013-01-03");

            _ = remoteCache.Put(4, account1);

            Account account2 = new Account();
            account2.Id = 2;
            account2.Description = "John Doe's second bank account";
            account2.CreationDate = MakeDate("2013-01-04");

            _ = remoteCache.Put(5, account2);

            Account account3 = new Account();
            account3.Id = 3;
            account3.CreationDate = MakeDate("2013-01-20");

            _ = remoteCache.Put(6, account3);
        }

        private void PutTransactions(Cache<Object, Object> remoteCache)
        {
            Transaction transaction0 = new Transaction();
            transaction0.Id = 0;
            transaction0.Description = "Birthday present";
            transaction0.AccountId = 1;
            transaction0.Amount = 1800;
            transaction0.Date = MakeDate("2012-09-07");
            transaction0.IsDebit = false;
            transaction0.IsValid = true;

            _ = remoteCache.Put(7, transaction0);

            Transaction transaction1 = new Transaction();
            transaction1.Id = 1;
            transaction1.Description = "Feb. rent payment";
            transaction1.AccountId = 1;
            transaction1.Amount = 1500;
            transaction1.Date = MakeDate("2013-01-05");
            transaction1.IsDebit = true;
            transaction1.IsValid = true;

            _ = remoteCache.Put(8, transaction1);

            Transaction transaction2 = new Transaction();
            transaction2.Id = 2;
            transaction2.Description = "Starbucks";
            transaction2.AccountId = 1;
            transaction2.Amount = 23;
            transaction2.Date = MakeDate("2013-01-09");
            transaction2.IsDebit = true;
            transaction2.IsValid = true;

            _ = remoteCache.Put(9, transaction2);

            Transaction transaction3 = new Transaction();
            transaction3.Id = 3;
            transaction3.Description = "Hotel";
            transaction3.AccountId = 2;
            transaction3.Amount = 45;
            transaction3.Date = MakeDate("2013-02-27");
            transaction3.IsDebit = true;
            transaction3.IsValid = true;

            _ = remoteCache.Put(10, transaction3);

            Transaction transaction4 = new Transaction();
            transaction4.Id = 4;
            transaction4.Description = "Last january";
            transaction4.AccountId = 2;
            transaction4.Amount = 95;
            transaction4.Date = MakeDate("2013-01-31");
            transaction4.IsDebit = true;
            transaction4.IsValid = true;

            _ = remoteCache.Put(11, transaction4);

            Transaction transaction5 = new Transaction();
            transaction5.Id = 5;
            transaction5.Description = "-Popcorn";
            transaction5.AccountId = 2;
            transaction5.Amount = 4;
            transaction5.Date = MakeDate("2013-01-01");
            transaction5.IsDebit = true;
            transaction5.IsValid = true;

            _ = remoteCache.Put(12, transaction5);
        }

        public ulong MakeDate(String date)
        {
            //For compatibility with Java side, use the number of milliseconds since 
            //January 1, 1970, 00:00:00. The time zone is not taken into account
            //in this example.
            DateTime inception = new DateTime(1970, 1, 1, 0, 0, 0);
            DateTime current = DateTime.Parse(date);
            return (ulong)current.Subtract(inception).TotalMilliseconds;
        }

    }


    [Collection("MainSequence")]
    public class RemoteQueryTest : IClassFixture<RemoteQueryTestFixture>
    {

        private readonly RemoteQueryTestFixture _fixture;
        private Cache<Object, Object> userCache;
        public RemoteQueryTest(RemoteQueryTestFixture fixture)
        {
            _fixture = fixture;
            userCache = _fixture.cache;
        }

        [Fact]
        public void EmptyTest()
        {
        }
        [Fact]
        public void GetAllTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Equal(3, listOfUsers.Count);
        }

        [Fact]
        public void Eq1Test()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.name = \"John\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal("John", listOfUsers.ElementAt(0).Name);
            Assert.Equal("Doe", listOfUsers.ElementAt(0).Surname);
        }
        [Fact]
        public void EqEmptyStringTest()
        {
            QueryRequest qr = new QueryRequest();
            // JpqlString will be deprecated please use QueryString
            // qr.JpqlString = "from sample_bank_account.User u where u.name = \"\"";
            qr.QueryString = "from sample_bank_account.User u where u.name = \"\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Empty(listOfUsers);
        }
        [Fact]
        public void EqSentenceTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Account a where a.description = \"John Doe's first bank account\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfAccounts = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfAccounts);
            Assert.Equal(1, listOfAccounts.ElementAt(0).Id);
        }
        [Fact]
        public void EqNonIndexedFieldTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.notes = \"Lorem ipsum dolor sit amet\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal(1, listOfUsers.ElementAt(0).Id);
        }

        [Fact]
        public void EqHybridQueryWithParamTest()
        {

            QueryRequest.Types.NamedParameter param = new QueryRequest.Types.NamedParameter();
            WrappedMessage wm = new WrappedMessage();
            wm.WrappedString = "Doe";
            param.Name = "surnameParam";
            param.Value = wm;

            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where (u.notes = \"Lorem ipsum dolor sit amet\") and (u.surname = :surnameParam)";
            qr.NamedParameters.Add(param);

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal(1, listOfUsers.ElementAt(0).Id);
        }

        [Fact]
        public void EqInNested1Test()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.addresses.postCode = \"X1234\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal("X1234", listOfUsers.ElementAt(0).Addresses.ElementAt(0).PostCode);
        }

        [Fact]
        public void LikeTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Transaction t where t.description like \"%rent%\"";
            QueryResponse result = userCache.Query(qr).Result;
            List<Transaction> listOfTx = RemoteQueryUtils.unwrapResults<Transaction>(result);

            Assert.Single(listOfTx);
            Assert.Equal(1, listOfTx.ElementAt(0).AccountId);
            Assert.Equal(1500, listOfTx.ElementAt(0).Amount, 0);
        }

        [Fact]
        public void BetweenTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Transaction t where t.date between \"" + _fixture.MakeDate("2013-01-01") + "\" and \"" + _fixture.MakeDate("2013-01-31") + "\"";
            QueryResponse result = userCache.Query(qr).Result;
            List<Transaction> listOfTx = RemoteQueryUtils.unwrapResults<Transaction>(result);

            Assert.Equal(4, listOfTx.Count);
            foreach (Transaction tx in listOfTx)
            {
                Assert.True(tx.Date >= _fixture.MakeDate("2013-01-01") && tx.Date <= _fixture.MakeDate("2013-01-31"));
            }
        }
        [Fact]
        public void GreaterThanTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Transaction t where t.amount > 1500";
            QueryResponse result = userCache.Query(qr).Result;
            List<Transaction> listOfTx = RemoteQueryUtils.unwrapResults<Transaction>(result);

            Assert.Single(listOfTx);
            Assert.True(listOfTx.ElementAt(0).Amount > 1500);
        }

        [Fact]
        public void OrTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where (u.surname = \"Man\") or (u.surname = \"Woman\")";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Equal(2, listOfUsers.Count);
            foreach (User u in listOfUsers)
            {
                Assert.Equal("Spider", u.Name);
            }
        }

        [Fact]
        public void NotTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.name != \"Spider\"";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal("John", listOfUsers.ElementAt(0).Name);
        }

        [Fact]
        public async void InvalidEmbeddedAttributeTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select u.addresses from sample_bank_account.User u";

            var excp = await Assert.ThrowsAsync<InfinispanException>(() => userCache.Query(qr));
            Assert.Equal("org.infinispan.objectfilter.ParsingException: ISPN028503: Property addresses can not be selected from type sample_bank_account.User since it is an embedded entity.", excp.Message);
        }
        [Fact]
        public void RejectProjectionOfRepeatedPropertyTest()
        {
            QueryRequest qr = new QueryRequest();
            // JpqlString will be deprecated please use QueryString
            // qr.JpqlString = "select u.addresses.postcode from sample_bank_account.User u";
            qr.QueryString = "select u.addresses.postcode from sample_bank_account.User u";

            Assert.ThrowsAsync<InfinispanException>(() => userCache.Query(qr));
        }
        [Fact]
        public void ProjectionTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select u.name, u.surname, u.age from sample_bank_account.User u where u.age is null";

            QueryResponse result = userCache.Query(qr).Result;
            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);
            Assert.Equal("Spider", projections.ElementAt(0)[0]);
            Assert.Equal("Man", projections.ElementAt(0)[1]);
            Assert.Equal("Spider", projections.ElementAt(1)[0]);
            Assert.Equal("Woman", projections.ElementAt(1)[1]);
        }

        [Fact]
        public void ContainsTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 2";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal("John", listOfUsers.ElementAt(0).Name);
        }

        [Fact]
        public void ContainsAllTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 1 and u.accountIds = 2";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfUsers);
            Assert.Equal(1, listOfUsers.ElementAt(0).Id);
        }

        [Fact]
        public void NotContainsAllTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 1 and u.accountIds = 2 and u.accountIds = 3";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Empty(listOfUsers);
        }

        [Fact]
        public void NotContainsTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 42";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Empty(listOfUsers);
        }

        [Fact]
        public void ContainsAnyTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 2 or u.accountIds = 3 order by u.id asc";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Equal(2, listOfUsers.Count);
            Assert.Equal(1, listOfUsers.ElementAt(0).Id);
            Assert.Equal(2, listOfUsers.ElementAt(1).Id);
        }

        [Fact]
        public void NotContainsAnyTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.accountIds = 4 or u.accountIds = 5";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Empty(listOfUsers);
        }

        [Fact]
        public void InTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.id in (1, 3)";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Equal(2, listOfUsers.Count);
        }

        [Fact]
        public void NotInTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u where u.id in (4)";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfUsers = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Empty(listOfUsers);
        }

        [Fact]
        public void StringLiteralEscapeTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Account a where a.description = 'John Doe''s first bank account'";

            QueryResponse result = userCache.Query(qr).Result;
            List<User> listOfAccounts = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Single(listOfAccounts);
            Assert.Equal(1, listOfAccounts.ElementAt(0).Id);
        }

        [Fact]
        public void HavingWithSumTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select t.accountId, sum(t.amount) from sample_bank_account.Transaction t group by t.accountId having sum(t.amount) > 3300";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Single(projections);
            Assert.Equal(1, projections.ElementAt(0)[0]);
            Assert.Equal(3323.0, (double)projections.ElementAt(0)[1], 3);
        }

        [Fact]
        public void HavingWithAvgTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select t.accountId, avg(t.amount) from sample_bank_account.Transaction t group by t.accountId having avg(t.amount) < 100";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Single(projections);
            Assert.Equal(2, projections.ElementAt(0)[0]);
            Assert.Equal(48.0, (double)projections.ElementAt(0)[1], 3);
        }

        [Fact]
        public void HavingWithMinTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select t.accountId, min(t.amount) from sample_bank_account.Transaction t group by t.accountId having min(t.amount) < 10";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Single(projections);
            Assert.Equal(2, projections.ElementAt(0)[0]);
            Assert.Equal(4.0, (double)projections.ElementAt(0)[1], 3);
        }

        [Fact]
        public void HavingWithMaxTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select t.accountId, max(t.amount) from sample_bank_account.Transaction t group by t.accountId having max(t.amount) > 1000";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Single(projections);
            Assert.Equal(1, projections.ElementAt(0)[0]);
            Assert.Equal(1800.0, (double)projections.ElementAt(0)[1], 3);
        }

        [Fact]
        public void GlobalSumTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select sum(t.amount) from sample_bank_account.Transaction t";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Single(projections);
            Assert.Equal(3467.0, (double)projections.ElementAt(0)[0], 3);
        }

        [Fact]
        public void CountTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select u.name, count(u.age) from sample_bank_account.User u group by u.name";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Equal(2, projections.Count);
            Assert.Equal("John", projections.ElementAt(0)[0]);
            Assert.Equal(1, (Int64)projections.ElementAt(0)[1]);
            Assert.Equal("Spider", projections.ElementAt(1)[0]);
        }

        [Fact]
        public void CountTestWithNull()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "select u.name, count(u.age) from sample_bank_account.User u group by u.name";

            QueryResponse result = userCache.Query(qr).Result;

            List<Object[]> projections = RemoteQueryUtils.unwrapWithProjection(result);

            Assert.Equal(2, projections.Count);
            Assert.Equal("John", projections.ElementAt(0)[0]);
            Assert.Equal(1, (Int64)projections.ElementAt(0)[1]);
            Assert.Equal("Spider", projections.ElementAt(1)[0]);
            Assert.Equal(0, (Int64)projections.ElementAt(1)[1]);
        }

        [Fact]
        public void SampleDomainQueryWith2SortingOptionsTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.User u order by u.name DESC, u.surname ASC";

            QueryResponse result = userCache.Query(qr).Result;

            List<User> list = RemoteQueryUtils.unwrapResults<User>(result);

            Assert.Equal(3, list.Count);
            Assert.Equal("Spider", list.ElementAt(0).Name);
            Assert.Equal("Man", list.ElementAt(0).Surname);
            Assert.Equal("Spider", list.ElementAt(1).Name);
            Assert.Equal("Woman", list.ElementAt(1).Surname);
            Assert.Equal("John", list.ElementAt(2).Name);
        }

        [Fact]
        public void SortByDateTest()
        {
            QueryRequest qr = new QueryRequest();
            qr.QueryString = "from sample_bank_account.Transaction t order by t.date ASC";
            QueryResponse result = userCache.Query(qr).Result;
            List<Transaction> listOfTx = RemoteQueryUtils.unwrapResults<Transaction>(result);

            Assert.Equal(_fixture.MakeDate("2012-09-07"), listOfTx.ElementAt(0).Date);
            Assert.Equal(_fixture.MakeDate("2013-02-27"), listOfTx.ElementAt(listOfTx.Count - 1).Date);
        }
    }
}
