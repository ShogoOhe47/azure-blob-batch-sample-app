using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs.Models;

namespace AzureBlobBatchApp
{
    /*
     * BlobBatchClient で操作を行うクラス
     * 
     * 命令ファイル
     * >CMD \t BlobPath
     * 
     * CMD: LIST, DELETE, HOT, COLD, ARCHIVE
     */

    internal class BlobBatchOperaterAsync
    {
        //
        string storageAccountName;
        string blobContainerName;
        BlobServiceClient blobServiceClient;
        BlobBatchClient blobBatchClient;
        string blobBaseUri;

        //
        string logDirPath;

        // 
        List<string> cmd_list_blobpath_list;
        List<string> cmd_list_blobpath_tier_hot;
        List<string> cmd_list_blobpath_tier_cold;
        List<string> cmd_list_blobpath_tier_archive;
        List<string> cmd_list_blobpath_delete;


        public BlobBatchOperaterAsync(string accountName, string containerName, string logDir)
        {
            storageAccountName = accountName;
            blobContainerName = containerName;
            logDirPath = logDir;

            // ログイン情報を使用してストレージアカウントへの接続、コンテナの取得を実施
            // https://learn.microsoft.com/ja-jp/azure/storage/blobs/storage-quickstart-blobs-dotnet?tabs=visual-studio%2Cmanaged-identity%2Croles-azure-portal%2Csign-in-azure-cli%2Cidentity-visual-studio
            blobServiceClient = new BlobServiceClient(
                new Uri($"https://{accountName}.blob.core.windows.net"),
                new DefaultAzureCredential());
            blobBatchClient = blobServiceClient.GetBlobBatchClient();

            blobBaseUri = $"https://{blobServiceClient.AccountName}.blob.core.windows.net/{containerName}";


            cmd_list_blobpath_list = new List<string>();
            cmd_list_blobpath_tier_hot = new List<string>();
            cmd_list_blobpath_tier_cold = new List<string>();
            cmd_list_blobpath_tier_archive = new List<string>();
            cmd_list_blobpath_delete = new List<string>();

        }

        public void readCMDFile(string fileName)
        {
            string CMDFilePath = Path.Join(logDirPath, fileName);

            // file check
            if (System.IO.File.Exists(CMDFilePath))
            {
                Console.WriteLine($"read path {CMDFilePath} is exist.");
            } else
            {
                Console.WriteLine($"[Error] read path {CMDFilePath} is not exist.");
                return;
            }


            // read
            string cmd = "";
            string blobPath = "";
            char[] splitSpearator = {' ', '\t'};
            foreach (string line in System.IO.File.ReadLines(CMDFilePath))
            {
                if (line.StartsWith('#')) {
                    continue;
                }

                string[] spritedLine = line.Split(splitSpearator, StringSplitOptions.RemoveEmptyEntries);
                if (spritedLine.Length < 2)
                {
                    Console.WriteLine($"Error: cmd line is invalid, not contain cmd: '{line}'");
                    continue;
                }

                cmd = spritedLine[0];
                blobPath = spritedLine[1];

                // CMD: LIST
                if ("LIST".Equals(cmd))
                {
                    cmd_list_blobpath_list.Add(blobPath);
                    continue;
                }

                // CMD: HOT, COLD, ARCHIVE
                if ("HOT".Equals(cmd))
                {
                    cmd_list_blobpath_tier_hot.Add(blobPath);
                    continue;
                }
                if ("COLD".Equals(cmd))
                {
                    cmd_list_blobpath_tier_cold.Add(blobPath);
                    continue;
                }
                if ("ARCHIVE".Equals(cmd))
                {
                    cmd_list_blobpath_tier_archive.Add(blobPath);
                    continue;
                }

                // CMD: DELETE
                if ("DELETE".Equals(cmd))
                {
                    cmd_list_blobpath_delete.Add(blobPath);
                    continue;
                }

                // ここまで来たら CMD が実行されていない
                Console.WriteLine($"Error: cmd line is invalid, invalid cmd: '{line}'");
            }
        }

        public void execBatch()
        {
            // create BatchClient
            blobBatchClient = blobServiceClient.GetBlobBatchClient();

            // LIST
            if (cmd_list_blobpath_list.Count() > 0)
            {
                Console.WriteLine($"[execBatch] LIST: {cmd_list_blobpath_list.Count()} items");
                execBlobList(blobContainerName);
            }
            else
            {
                Console.WriteLine($"[execBatch] LIST: no items. skip.");
            }

            // Blob Tier: HOT, COLD, ARCHIVE
            if (cmd_list_blobpath_tier_hot.Count() > 0)
            {
                // HOT
                Console.WriteLine($"[execBatch] HOT: {cmd_list_blobpath_tier_hot.Count()} items");
                execBlobTierChange(AccessTier.Hot, cmd_list_blobpath_tier_hot);
            }
            else
            {
                Console.WriteLine($"[execBatch] HOT: no items. skip.");
            }
            if (cmd_list_blobpath_tier_cold.Count() > 0)
            {
                // COLD
                Console.WriteLine($"[execBatch] COLD: {cmd_list_blobpath_tier_cold.Count()} items");
                execBlobTierChange(AccessTier.Cold, cmd_list_blobpath_tier_cold);
            }
            else
            {
                Console.WriteLine($"[execBatch] COLD: no items. skip.");
            }
            if (cmd_list_blobpath_tier_archive.Count() > 0)
            {
                // ARCHIVE
                Console.WriteLine($"[execBatch] ARCHIVE: {cmd_list_blobpath_tier_archive.Count()} items");
                execBlobTierChange(AccessTier.Archive, cmd_list_blobpath_tier_archive);
            }
            else
            {
                Console.WriteLine($"[execBatch] ARCHIVE: no items. skip.");
            }

            // DELETE
            if (cmd_list_blobpath_delete.Count() > 0)
            {
                Console.WriteLine($"[execBatch] DELETE: {cmd_list_blobpath_delete.Count()} items");
                execBlobDelete();
            }
            else
            {
                Console.WriteLine($"[execBatch] DELETE: no items. skip.");
            }
        }

        private void execBlobList(string containerName, string reportFileNamePrefix = "azure-blob-list_", string reportFileNameSuffix = "", string reportFileNameExt = "log")
        {
            string reportFileName = "";

            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            string dtStr = DateTime.Now.ToString("yyyyMMdd-HHmmss");
            reportFileName = $"{reportFileNamePrefix}{dtStr}{reportFileNameSuffix}.{reportFileNameExt}";
            string reportFilePath = Path.Join(logDirPath, reportFileName);

            try
            {
                // 出力先のオープン
                using (StreamWriter streamWriter = new StreamWriter(reportFilePath))
                {
                    Console.WriteLine($"blob report stream is output to: '{reportFilePath}'");
                    // blob の個別の情報を書き出す
                    int count = 0;

                    foreach (string blobPath in cmd_list_blobpath_list) {
                        Console.WriteLine($" checking for: container='{containerName}', blob='{blobPath}'");

                        foreach (BlobItem blobItem in containerClient.GetBlobs(prefix: blobPath))
                        {
                            string blobName = blobItem.Name;
                            long? blobLength = blobItem.Properties.ContentLength;
                            AccessTier? blobTier = blobItem.Properties.AccessTier;
                            string blobMetaMD5 = Convert.ToBase64String(blobItem.Properties.ContentHash);

                            string line = $"{blobName}\t{blobLength}\t{blobTier}\t{blobMetaMD5}";
                            streamWriter.WriteLine(line);
                            count++;
                            if (count % 3000 == 0)
                            {
                                Console.WriteLine($"blob items write count: {count}");
                            }
                        }
                    }

                    Console.WriteLine($"blob report is EOL: total item count={count}");
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
            }

            // サマリの生成
            calcBlobSizeDirSummary(reportFilePath, reportFileNameDatetime: dtStr);
        }

        private void calcBlobSizeDirSummary(string logFilePath, string reportFileNamePrefix = "azure-blob-list_", string reportFileNameDatetime = "", string reportFileNameSuffix = "_summary", string reportFileNameExt = "log")
        {
            // <親フォルダ, [アイテム数, サイズ]> で格納する
            Dictionary<String, List<long>> blobDirSize = new Dictionary<String, List<long>>();

            char[] splitSpearator = { '\t' }; // ' ' は "abc (2).txt" でダメだったので避ける
            foreach (string line in System.IO.File.ReadLines(logFilePath))
            {
                string[] spritedLine = line.Split(splitSpearator, StringSplitOptions.RemoveEmptyEntries);
                if (spritedLine.Length < 2)
                {
                    Console.WriteLine($"Error: blob list file is invalid: '{line}'");
                    continue;
                }

                string blobPath = spritedLine[0];
                long blobSize = long.Parse(spritedLine[1]);
                string? parentDirName = "";

                // 親フォルダの特定
                try
                {
                    parentDirName = Path.GetDirectoryName(blobPath);
                } catch (ArgumentException e)
                {
                    Console.WriteLine($"{e.Message}");
                } catch (PathTooLongException e)
                {
                    Console.WriteLine($"{e.Message}");
                }
                
                // 辞書の登録
                if (blobDirSize.ContainsKey(parentDirName))
                {
                    // 辞書の更新
                    List<long> current = blobDirSize[parentDirName];

                    // [アイテム数, サイズ]
                    current[0] = current[0]+1;
                    current[1] = current[1] + blobSize;

                    blobDirSize[parentDirName] = current;
                }
                else
                {
                    // 辞書への新規登録
                    List<long> newList = new List<long>();
                    newList.Add(1);
                    newList.Add(blobSize);

                    blobDirSize[parentDirName] = newList;
                }
            }

            // 書き出し
            string reportSummaryFileName = $"{reportFileNamePrefix}{reportFileNameDatetime}{reportFileNameSuffix}.{reportFileNameExt}";
            string reportSummaryFilePath = Path.Join(logDirPath, reportSummaryFileName);
            try
            {
                // 出力先のオープン
                using (StreamWriter streamWriter = new StreamWriter(reportSummaryFilePath))
                {
                    Console.WriteLine($"blob summary report stream is output to: '{reportSummaryFilePath}'");
                    // 辞書の個別の情報を書き出す
                    foreach (KeyValuePair<string, List<long>> item in blobDirSize)
                    {
                        string path = item.Key;
                        long numOfItems = item.Value[0];
                        long size = item.Value[1];

                        string line = $"{path}\t{numOfItems}\t{size}";
                        streamWriter.WriteLine(line);
                    }

                    Console.WriteLine($"blob summary report is EOL.");
                }
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
            }

        }

        private List<Uri> blobBatchUriBuilder(string baseUri, List<string> pathList)
        {
            List<Uri> blobUriList = new List<Uri>();

            // 入力された pathList から uri を作成する
            foreach (string path in pathList)
            {
                // TODO: exist check -> slow down, only pre-check?
                UriBuilder uriBuilder = new UriBuilder(baseUri + "/" + path);
                blobUriList.Add(uriBuilder.Uri);
            }

            return blobUriList;
        }


        private int blobBatchCheckBatchSize(int windowSize, int maxWindowSize = 255)
        {
            // バッチで使うウィンドウサイズ (blobの数) を 1~255 の間で調整
            int newWindowSize = windowSize;

            if (windowSize > maxWindowSize)
            {
                newWindowSize = maxWindowSize;
            }
            else if (windowSize < 1)
            {
                newWindowSize = 1;
            }

            return newWindowSize;
        }

         private void execBlobTierChange(AccessTier accessTier, List<string> blobPathList, int batchWindowSize = 250, RehydratePriority rehydratePriority = Azure.Storage.Blobs.Models.RehydratePriority.Standard)
        {
            Console.WriteLine($"Blob tier change: move to {accessTier}, {blobPathList.Count()} items.");
            Console.WriteLine($"  batch window size = {batchWindowSize}, priority = {rehydratePriority.ToString()}");

            List<Uri> blobUriList = blobBatchUriBuilder(blobBaseUri, blobPathList);
            int numOfItemsPerPage = blobBatchCheckBatchSize(batchWindowSize);

            // exec part
            var chunks = blobUriList.Select((v, i) => new { v, i })
                .GroupBy(x => x.i / numOfItemsPerPage)
                .Select(g => g.Select(x => x.v));

            // numOfItemsPerPage の件数で分割してバッチを実行
            // count は処理済の件数
            int count = 0;
            foreach (var chunk in chunks)
            {
                if (count % 1000 == 0)
                {
                    Console.WriteLine($"BlobBatch is processing... : {count} items");
                }
                blobBatchClient.SetBlobsAccessTier(chunk, AccessTier.Hot, rehydratePriority);
                count+= numOfItemsPerPage;
            }
        }

        private void execBlobDelete(int batchWindowSize = 250)
        {
            Console.WriteLine($"Delete blob items {cmd_list_blobpath_delete.Count()} items.");
            Console.WriteLine($"  batch window size = {batchWindowSize}");

            List<Uri> blobUriList = blobBatchUriBuilder(blobBaseUri, cmd_list_blobpath_delete);
            int numOfItemsPerPage = blobBatchCheckBatchSize(batchWindowSize);

            // exec part
            var chunks = blobUriList.Select((v, i) => new { v, i })
                .GroupBy(x => x.i / numOfItemsPerPage)
                .Select(g => g.Select(x => x.v));

            // numOfItemsPerPage の件数で分割してバッチを実行
            // count は処理済の件数
            int count = 0;
            foreach (var chunk in chunks)
            {
                if (count % 3000 == 0)
                {
                    Console.WriteLine($"BlobBatch is processing... : {count} items");
                }
                blobBatchClient.DeleteBlobs(chunk);
                count += numOfItemsPerPage;
            }
        }

    }
}
