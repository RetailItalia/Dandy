using Dandy.Mapping;
using IBM.Data.DB2.Core;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dandy.Examples.Core
{
    class Program
    {
        private const string ConnectionString = "Server=127.0.0.1:50000; Database=TESTDB; UID=DB2INST1; PWD=password";
        private static DB2Connection GetConnection() => new DB2Connection(ConnectionString);

        static async Task Main(string[] args)
        {
            SqlMapperExtensions.InitMapping();
            using (var conn = GetConnection())
            {
                var pencil = new Article { Id = 2, Name = "Pencil", Description = "my pencil" };

                await conn.OpenAsync();

                await conn.InsertAsync(pencil);

                var myPencil = await conn.GetAsync<Article>(pencil.Id);
                Console.WriteLine($"{myPencil.Name}");

                for (int i = 10; i < 100; i++)
                    await conn.InsertAsync(new Article { Id = i, Name = $"Pencil_{i}", Description = $"my pencil {i}" });

                var getAllResult = await conn.GetAllAsync<Article>();
                getAllResult.ToList().ForEach(a => Console.WriteLine($"{a.Name}"));

                var getAllFiltered = await conn.GetAllAsync<Article>(filter: article => article.Name.EndsWith("2"));
                getAllFiltered.ToList().ForEach(a => Console.WriteLine($"{a.Name}"));

                var getAllPaged = await conn.GetAllAsync<Article>(page: 3, pageSize: 10);
                getAllPaged.ToList().ForEach(a => Console.WriteLine($"{a.Name}"));
            }
        }
    }

    public class Article
    {
        [ExplicitKey]
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    public class AliasMapper : EntityMap<Article>
    {
        public AliasMapper()
        {
            ToTable("ARTICO0F");
            Map(_ => _.Id).ToColumn("ARTID");
            Map(_ => _.Name).ToColumn("ARTNAME");
            Map(_ => _.Description).ToColumn("ARTDESCR");
        }
    }
}