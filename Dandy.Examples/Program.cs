using Dandy.Mapping;
using IBM.Data.DB2.iSeries;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dandy.Examples
{
    class Program
    {
        private const string DBConnectionString = "db connection string";
        static void Main(string[] args)
        {
            GetAll().GetAwaiter().GetResult();
            WithFilter().GetAwaiter().GetResult();
            WithMapping().GetAwaiter().GetResult();
            WithPagination().GetAwaiter().GetResult();
        }

        private static async Task GetAll()
        {
            SqlMapperExtensions.InitMapping();
            using (var connection = new iDB2Connection(DBConnectionString))
            {
                await connection.OpenAsync();
                var items = await connection.GetAllAsync<Article>();

                items
                    .ToList()
                    .ForEach(
                        proc => Console.WriteLine($"ID: {proc.Id} Name: {proc.Name} Description: {proc.Description}"
                    ));
            }
        }

        private static async Task WithFilter()
        {
            SqlMapperExtensions.InitMapping();
            using (var connection = new iDB2Connection(DBConnectionString))
            {
                await connection.OpenAsync();
                var items = await connection.GetAllAsync<Article>(filter: x => x.Name == "AR1");

                items
                    .ToList()
                    .ForEach(
                        proc => Console.WriteLine($"ID: {proc.Id} Name: {proc.Name} Description: {proc.Description}"
                    ));
            }
        }

        private static async Task WithMapping()
        {
            SqlMapperExtensions.InitMapping();
            using (var connection = new iDB2Connection(DBConnectionString))
            {
                await connection.OpenAsync();
                var items = await connection.GetAllAsync<Article>(filter: x => x.Name == "AR1");

                items
                    .ToList()
                    .ForEach(
                        proc => Console.WriteLine($"ID: {proc.Id} Name: {proc.Name} Description: {proc.Description}"
                    ));
            }
        }
        private static async Task WithPagination()
        {
            SqlMapperExtensions.InitMapping();
            using (var connection = new iDB2Connection(DBConnectionString))
            {
                await connection.OpenAsync();
                var items = await connection.GetAllAsync<Article>(page: 1, pageSize: 10);

                items
                    .ToList()
                    .ForEach(
                        proc => Console.WriteLine($"ID: {proc.Id} Name: {proc.Name} Description: {proc.Description}"
                    ));
            }
        }
    }

    public class Article
    {
        [Key]
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