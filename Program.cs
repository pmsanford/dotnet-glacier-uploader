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
        private const int ConcurrencyLimit = 20;

        static void Main(string[] args)
        {
            if (args[0] == "upload")
            {
                HandleUpload(args.Skip(1).ToArray());
            }
            if (args[0] == "inventory")
            {
                HandleInventory(args.Skip(1).ToArray());
            }
            if (args[0] == "statuswatch")
            {
                HandleStatus(args.Skip(1).ToArray());
            }
        }

        private static void HandleStatus(string[] args)
        {
            int interval;
            if (args.Length < 6 || !int.TryParse(args[4], out interval))
            {
                Console.WriteLine("args should be aws_key aws_secret vault_name job_id interval_secs output_filename");
                return;
            }
            var aws_key = args[0];
            var aws_secret = args[1];
            var vault_name = args[2];
            var job_id = args[3];
            var filename = args[5];
            var creds = new BasicAWSCredentials(aws_key, aws_secret);
            var config = new AmazonGlacierConfig
            {
                RegionEndpoint = RegionEndpoint.USWest1,
                Timeout = TimeSpan.FromDays(10)
            };
            var client = new AmazonGlacierClient(creds, config);
            var descReq = new DescribeJobRequest(vault_name, job_id);
            do
            {
                Console.WriteLine("Checking status...");
                var jobStatus = client.DescribeJobAsync(descReq).Result;
                if (jobStatus.Completed)
                {
                    Console.WriteLine("Job completed.");
                    break;
                }
                Console.WriteLine($"Job incomplete.");
                Console.WriteLine($"Job status: {jobStatus.StatusCode}");
                Thread.Sleep(interval);
            } while (true);
            var retrReq = new GetJobOutputRequest(vault_name, job_id, "bytes=0-1073741824");
            var retrievalPromise = client.GetJobOutputAsync(retrReq).Result;
            var json = new StreamReader(retrievalPromise.Body).ReadToEnd();
            File.WriteAllText(filename, json);
            Console.WriteLine($"Output written to {filename}");
        }

        private static void HandleInventory(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine($"args should be aws_key aws_secret vault_name output_filename");
                return;
            }
            var aws_key = args[0];
            var aws_secret = args[1];
            var vault_name = args[2];
            var filename = args[3];
            var creds = new BasicAWSCredentials(aws_key, aws_secret);
            var config = new AmazonGlacierConfig
            {
                RegionEndpoint = RegionEndpoint.USWest1,
                Timeout = TimeSpan.FromDays(10)
            };
            var client = new AmazonGlacierClient(creds, config);
            var initReq = new InitiateJobRequest(vault_name, new JobParameters("JSON", "inventory-retrieval", null, null));
            var promise = client.InitiateJobAsync(initReq);
            promise.ContinueWith(job =>
            {
                Console.WriteLine($"Job ID: {job.Result.JobId}");
                File.WriteAllText(filename, job.Result.JobId);
            });
            Console.WriteLine("Retrieval job initiated");
        }

        private static void HandleUpload(string[] args)
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
                    bool noErrors = true;
                    while (noErrors)
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
                            if (tsk.IsFaulted)
                            {
                                Console.WriteLine($"Exception encountered: {tsk.Exception.ToString()}");
                                noErrors = false;
                                throw tsk.Exception;
                            }
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
