using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace adlademo1
{
    class Program
    {
        private static string _adlaAccountName = "tsichouadla";

        private static DataLakeAnalyticsAccountManagementClient _adlaClient;
        private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;
        private static DataLakeAnalyticsJobManagementClient _adlaJobClient;

        static void Main(string[] args)
        {
            string localFolderPath = @"c:\Temp\";
            var tenantid = "72f988bf-86f1-41af-91ab-2d7cd011db47";
            var applicationid = "3d13d90c-ad44-4138-9dc7-8947cbac179b";
            var subscriptionid = "796eaf7c-2e26-4938-9b94-0f2369c33d74";
            var password = "e5wnO76YaeTFgsKBMSZ1Cdf3e/nPPmN6uVsg2ngNazI=";

            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var clientCredential = new ClientCredential(applicationid, password);
            var creds = ApplicationTokenProvider.LoginSilentAsync(tenantid, clientCredential).Result;

            // Only the Data Lake Analytics and Data Lake Store  
            // objects need a subscription ID.
           
            _adlaClient = new DataLakeAnalyticsAccountManagementClient(creds);
            _adlaClient.SubscriptionId = subscriptionid;

            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);
            _adlaJobClient = new DataLakeAnalyticsJobManagementClient(creds);

            var adlaAccounts = _adlaClient.Account.List().ToList();
            foreach (var adla in adlaAccounts)
            {
                Debug.WriteLine($"\t{adla.Name}");
            }

            string scriptPath = localFolderPath + "SampleUSQLScript.usql";
            string jobName = "My First ADLA Job";

            var script = File.ReadAllText(scriptPath);
            var jobId = Guid.NewGuid();
            var properties = new USqlJobProperties(script);
            var parameters = new JobInformation(jobName, JobType.USql, properties, priority: 1, degreeOfParallelism: 1, jobId: jobId);
            var jobInfo = _adlaJobClient.Job.Create(_adlaAccountName, jobId, parameters);

            var jobInfo1 = _adlaJobClient.Job.Get(_adlaAccountName, jobId);
            while (jobInfo1.State != JobState.Ended)
            {
                jobInfo1 = _adlaJobClient.Job.Get(_adlaAccountName, jobId);
            }

            WaitForNewline("Job completed.", "Downloading job output.");
            WaitForNewline("Job output downloaded. You can now exit.");

        }

        public static void WaitForNewline(string reason, string nextAction = "")
        {
            Console.WriteLine(reason + "\r\nPress ENTER to continue...");

            Console.ReadLine();

            if (!String.IsNullOrWhiteSpace(nextAction))
                Console.WriteLine(nextAction);
        }
    }
}
