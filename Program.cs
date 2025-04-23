using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Broker
{
    class Program
    {
        public static void Main(string[] args)
        {

            var builder = WebApplication.CreateBuilder(args);
            builder.Services.AddSingleton<BrokerService>();
            builder.Services.AddGrpc();

            builder.WebHost.ConfigureKestrel(options =>
            {
                options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
            });

            var app = builder.Build();


            app.MapGrpcService<BrokerService>();

            app.Run();

        }
    }
}
