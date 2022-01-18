

using Cassandra;

namespace foo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var cluster = Cluster.Builder()
                                .AddContactPoints("127.0.0.1")
                                .Build();
            using (var session = cluster.Connect("magic"))
            {
                var rs = session.Execute("select * from files");
                foreach (var row in rs)
                {
                    var value = row.GetValue<string>("path");
                    System.Console.WriteLine(value);
                }
            }
        }
    }
}