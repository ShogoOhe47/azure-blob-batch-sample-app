using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AzureBlobBatchApp
{
    class Settings
    {
#pragma warning disable CS8618 // null 非許容のフィールドには、コンストラクターの終了時に null 以外の値が入っていなければなりません。Null 許容として宣言することをご検討ください。
        public string StorageAccount { get; set; }
        public string BlobContainer { get; set; }
        public string BlobCMDDir { get; set; }
        public string BlobCMDFile { get; set; }
#pragma warning restore CS8618 // null 非許容のフィールドには、コンストラクターの終了時に null 以外の値が入っていなければなりません。Null 許容として宣言することをご検討ください。
    }

    internal class Program
    {
        static Settings appsettings = new Settings();

        static void ReadConfig(IHost host)
        {
            // Ask the service provider for the configuration abstraction.
            IConfiguration config = host.Services.GetRequiredService<IConfiguration>();

            // Get values from the config given their key and their target type.
            appsettings.StorageAccount = config.GetValue<string>("StorageAccount") ?? "";
            appsettings.BlobContainer = config.GetValue<string>("BlobContainer") ?? "";
            appsettings.BlobCMDDir = config.GetValue<string>("BlobCMDDir") ?? "";
            appsettings.BlobCMDFile = config.GetValue<string>("BlobCMDFile") ?? "";
        }


        static void Main(string[] args)
        {
            using IHost host = Host.CreateDefaultBuilder(args).Build();

            //Console.WriteLine("Hello, World!");
            ReadConfig(host);

            // Application code which might rely on the config could start here.
            // await host.RunAsync();

            // read CMD
            BlobBatchOperaterAsync blobBatchOperater = new BlobBatchOperaterAsync(appsettings.StorageAccount, appsettings.BlobContainer, appsettings.BlobCMDDir);
            blobBatchOperater.readCMDFile(appsettings.BlobCMDFile);
            blobBatchOperater.execBatch();
        }
    }
}