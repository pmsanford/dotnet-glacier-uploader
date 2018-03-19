using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Glacier;
using Amazon.Glacier.Model;
using Amazon.Glacier.Transfer;
using Amazon.Runtime;
using ByteSizeLib;

namespace glacierup
{
    class Program
    {
        private const long Megabytes = 1024 * 1024;
        private const long PartSize = 64 * Megabytes;
        private const int ConcurrencyLimit = 10;

        static void Main(string[] args)
        {
            if (args.Length < 5)
            {
                Console.WriteLine($"args should be aws_key aws_secret vault_name description filename");
                return;
            }
            var aws_key = args[0];
            var aws_secret = args[1];
            var vault_name = args[2];
            var description = args[3];
            var filename = args[4];
            var creds = new BasicAWSCredentials(aws_key, aws_secret);
            var config = new AmazonGlacierConfig
            {
                RegionEndpoint = RegionEndpoint.USWest1,
                Timeout = TimeSpan.FromDays(10)
            };
            var client = new AmazonGlacierClient(creds, config);
            var initReq = new InitiateMultipartUploadRequest(vault_name, description, PartSize);
            var ts = new CancellationTokenSource();
            var completed = 0;
            var started = 0;
            try
            {
                var res = client.InitiateMultipartUploadAsync(initReq, ts.Token).Result;
                var promises = new List<Task<UploadMultipartPartResponse>>();
                Task<UploadMultipartPartResponse> lastPart = null;
                var sem = new SemaphoreSlim(ConcurrencyLimit);
                long totalSize = 0;
                int totalParts = 0;
                using (var fs = new FileStream(filename, FileMode.Open))
                {
                    totalSize = fs.Length;
                    Console.WriteLine($"Preparing to upload {ByteSize.FromBytes(totalSize)}");
                    totalParts = (int)(fs.Length / PartSize) + 1;
                    while (true)
                    {
                        sem.Wait();
                        var arr = new byte[PartSize];
                        var start = fs.Position;
                        var read = fs.Read(arr, 0, (int)PartSize);
                        var check = TreeHasher.ComputeArrayHashString(arr, read);
                        var partReq = new UploadMultipartPartRequest(vault_name,
                                                                        res.UploadId,
                                                                        check,
                                                                        $"bytes {start}-{start + read - 1}/*",
                                                                        new MemoryStream(arr, 0, read));
                        var promise = client.UploadMultipartPartAsync(partReq, ts.Token);
                        Interlocked.Increment(ref started);
                        Console.WriteLine($"Started {started} out of {totalParts}");
                        promise.ContinueWith(tsk =>
                        {
                            Interlocked.Increment(ref completed);
                            Console.WriteLine($"{completed} out of {totalParts} completed.");
                            sem.Release();
                        });
                        promises.Add(promise);
                        if (read < PartSize || fs.Position >= fs.Length - 1)
                        {
                            lastPart = promise;
                            break;
                        }
                    }
                }

                Task.WaitAll(promises.ToArray());
                using (var fs = new FileStream(filename, FileMode.Open))
                {
                    var check = TreeHasher.ComputeHashString(fs);
                    var finisher = new CompleteMultipartUploadRequest(vault_name, res.UploadId, totalSize.ToString(), check);
                    Console.WriteLine("Finishing up");
                    Console.WriteLine($"Computed checksum {check}");
                    var result = client.CompleteMultipartUploadAsync(finisher, ts.Token).Result;
                    Console.WriteLine($"Completed: {result.Checksum}");
                    Console.WriteLine($"Calculated: {check}");
                    var match = string.Equals(result.Checksum, check, StringComparison.InvariantCultureIgnoreCase) ? "" : "not ";
                    Console.WriteLine($"Checksums do {match}match.");
                    Console.WriteLine($"Archive ID: {result.ArchiveId} Location: {result.Location}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception thrown: {ex.GetType().Name} - {ex.Message}");
                Console.WriteLine($"Full exception: {ex.ToString()}");
            }
        }
    }
}
