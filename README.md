
![Title Image](images/pipelineImage.PNG)

# Introduction
In this post, we highlight how to build a scalable machine learning-based data processing pipeline using [Microsoft R Server](https://www.microsoft.com/en-us/cloud-platform/r-server) with [Apache Spark](https://spark.apache.org/) utilizing [Azure Data Factory](https://azure.microsoft.com/en-us/documentation/articles/data-factory-introduction/) (ADF).  We provide step-by-step instructions and a customizable [Azure Resource Manager template](https://azure.microsoft.com/en-us/documentation/articles/resource-manager-template-walkthrough/) that provides one-click deployment of the entire solution.  
 
A data pipeline with Microsoft R makes sense when one uses R for processing large datasets at regular intervals. For e.g., running a stock market analysis model at the end of each business day or re-training/updating the predictive models every hour based on the new incoming labeled data. If there is a need for an ETL (Extraction, Transformation, and Load) step before the data reaches R, such an ETL step can also be added to the same pipeline as a dependency to the R task. This removes the need for synchronization as R task will wait on the dependencies to finish.
 
As an example dataset for this post, we use open source data from [NYC Taxi Dataset](http://www.andresmh.com/nyctaxitrips), public NYC Taxi Trip and Fare data-set. This dataset contains details on each taxi trip inclusing pick-up/drop-off location, fare, tip paid, etc. Using this data, we want to train a predictive model to predict the tip amount on each ride. We imagine, in the case of NY taxi, that the new trip and fare data is collected everyday and we want to regularly update our predictive model to incorporate the latest trends. We will build a pipeline that periodically reads the new data and runs it through R to create a new and updated predictive model.
 
The post emphasizes on how we can build a pipeline using Azure Data factory with Microsoft R over Apache Spark to schedule data processing jobs. We want to give readers a usable example that can be modified for their datasets and use-cases. For the sake of simplicity, we have skipped tuning and validation of our machine learning model.  
 
By the end of this post we want readers to be confident in:
 
1.	Utilizing Microsoft R and Spark cluster using Azure Data Factory (ADF) (with Azure Resource Templates) 
2.	Setting up custom R jobs using the ADF
3.	Manage the data movements when using Microsoft R with Spark


# Background 
 
There are three things that we should understand as we divulge into the details:
 
## Microsoft R:  
Microsoft provides an enhanced implementation on top of Open-source R that allows benefits of distributed computing to flow into R. The Microsoft R implements several popular machine learning algorithms to make them more efficient and scalable over distributed computing frameworks.  Apache Spark is one of the supported distributing computing platforms for Microsoft R.  The use of Apache Spark allows us to do computations on datasets in R that do not fit onto the memory of a single machine. 
 
The beauty of Microsoft R comes from the design that allows with a single line change (setting a suitable computing context) to switch the backend computing platform (from Hadoop to Apache Spark to local machine).  Rest of the code is agnostic and independent to the backend. We mainly use the functionality provided by the [RevoScaleR package](https://msdn.microsoft.com/en-us/microsoft-r/scaler-getting-started). More details on how to use R with Spark are in this [blog](https://blogs.msdn.microsoft.com/azuredatalake/2016/08/09/rapid-big-data-prototyping-with-microsoft-r-server-on-apache-spark-context-switching-spark-tuning/).
 
## Azure Data Factory (ADF):
Data factory allows users to create data pipelines and orchestrates data movement. In simple words, allows users to specify dependencies between data and computational tasks and create a time-based data flow. The usefulness of using ADF comes from the fact that it provides several inbuilt tools for creating a pipeline (on demand clusters, ability to run custom jobs) and provides insights into each step of a running pipeline. The documentation [here](https://azure.microsoft.com/en-us/documentation/services/data-factory/) provides details on ADF  . Similar to other services offered by Azure, ADF also allows one-click deployment through [Azure Resource Manager Templates](https://azure.microsoft.com/en-us/documentation/articles/resource-group-authoring-templates/).  ADF also has support to run Hadoop/Spark cluster as a PAAS and can also integrate with existing HDInsight clusters among other compute platforms. A typical ADF instance comprises of pipelines and each pipeline can contain multiple activities. Each activity takes at least one input dataset, a compute linked services to process the input and produce at least one output dataset. A [compute linked service](https://azure.microsoft.com/en-us/documentation/articles/data-factory-compute-linked-services/) is a facility that allows connecting ADF to computer resources such Spark cluster or Azure Batch. 

Currently, ADF only supports Map-reduce jobs (no customs jobs) on Linux HDI clusters and since Microsoft R runs only on Linux cluster, as a workaround, we will use a [custom task-runner](https://github.com/Azure/Azure-DataFactory/tree/master/Samples/CustomizedJavaOnHDInsight) masquerading as a map-reduce job to run Microsoft R.    

## Dataset:
The 2013 [NYC dataset](http://www.andresmh.com/nyctaxitrips) in the available form has two csv files for Trips and Fares. For the predictive use, we process and join them as per the instruction in this [post](https://azure.microsoft.com/en-us/documentation/articles/machine-learning-data-science-process-hive-walkthrough).  <p>TODO: udayan will add more details <p> To speed up the demo, we are only using data from one of the months. One can take similar steps to run this demo code on larger datasets. 

In the following sections, we provide steps to create an ADF pipeline with Microsoft R to for generating a machine learning model on the NYC taxi dataset.  



# Setup
1.	 Create an [Azure account](https://azure.microsoft.com/en-us/) (Skip if already have an azure account)
2.	Setup any one of [several available template deployment methods](https://azure.microsoft.com/en-us/documentation/articles/resource-group-template-deploy/). 
It's also possible to deploy templates through [Azure portal](https://azure.microsoft.com/en-us/documentation/articles/resource-group-template-deploy-portal/#deploy-resources-from-custom-template), minimizing the setup time. We use PowerShell based deployment through this blog.
3.	Before starting the pipeline, we must make the dataset available to the R-server/Spark cluster. Every Spark Cluster on Azure is associated with at least one storage account. Below are the steps on how to create a Storage Account and then make the dataset/scripts available on this storage account.

    a.  Create a [storage account](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/) in Azure via Azure portal within a new Resource Group. We will create the cluster in this resource group. We will use this Resource Group later when deploying the arm template. Select a location that supports all the resources needed - storage, and HDI cluster. "South Central US" is one such location that has all the resources.   General purpose Standard storage container with LRS replication is suitable for this work. 
    
    b.  Create a Blob container in this storage account. We will copy dataset to this container and later attach this container to the R/Spark cluster. 
   
    c.  Note the Storage Account Name and Access Keys from the portal after account creation.
   
    d.  Install [Azure Storage Explorer](https://azure.microsoft.com/en-us/documentation/articles/vs-azure-tools-storage-manage-with-storage-explorer/)
   
    e.  [Attach](https://azure.microsoft.com/en-us/documentation/articles/vs-azure-tools-storage-manage-with-storage-explorer/#attach-or-detach-an-external-storage-account) the Storage Account to Storage Explorer using the Name and Access Keys obtained in Step 3c 
   
    f.  Attach the public account using Shared Access Signature(SAS) URI to Azure Storage Explorer using this URI: https://dataforadfmrsdemo.blob.core.windows.net/demofiles?st=2016-09-08T18%3A17%3A00Z&se=2030-09-09T18%3A17%3A00Z&sp=rl&sv=2015-04-05&sr=c&sig=CAk9%2Bwe6pne%2BUoVmQbXv54ULYo1X9EaLsmf3vFZmV%2BY%3D
   
    g.  Download the 'data' and 'script' folders from the SAS attached blob container - 'Demofiles' 
   
    h.  Upload these files to the private container created in Step 3b

4.	Download [ADF-Rserver-apache-spark-pipeline.template.json](ADF-Rserver-apache-spark-pipeline.template.json) and [ADF-Rserver-apache-spark-pipeline.parameters.json](ADF-Rserver-apache-spark-pipeline.parameters.json) .  The first file is the Azure Resource Management template file that contains information on the resources and the constraints. The second file is a parameter file that contains all the configuration parameters including passwords and keys. Edit this parameter file and replace 'parameterStorageAccountName', 'parameterStorageAccountAccessKey', and 'parameterStorageContainerName' with the values from the newly created storage account. We also strongly recommend changing the passwords (10 characters long with at least a digit, a special character, a lower-case alphabet, and an upper-case alphabet).  


# Starting the pipeline:
 
Azure allows deploying resources and configurations through [Azure Resource Management templates](https://azure.microsoft.com/en-us/documentation/articles/resource-manager-template-walkthrough/). We will show steps for  template based deployment. The template will start the HDInsight’s Spark Cluster, install Microsoft R server, and finally create Azure data factory. Once the resource deployment is complete, the pipeline will start running based on the temporal configurations (currently set to once a month). The advantage of template deployment is speed of deployment and repeatability. For our current scenario, we store the configuration parameters such as the ssh username/password, in a separate parameter file that is passed along with the template for deployment. Below are the steps for deploying the template using Windows PowerShell and what to expect after deployment. 

![Relationship among Azure Data Factory component](images/ADF-flow-diagram.PNG)

*Figure 1.* Relationship among Azure Data Factory component

## Deployment
1.  Login to Azure using PowerShell 

    **Login-AzureRmAccount**
2. List all available subscription

    **Get-AzureRmSubscription**

3.	Select an Azure subscription

    **Select-AzureRmSubscription  -SubscriptionId ```<Subscription ID>```**
4.	Using the Resource Group name used for creating the Storage Account, deploy the arm template. The 'NameForTheDeployment' can be any string to identify the deployment

    **New-AzureRmResourceGroupDeployment -Name ```<NameForTheDeployment>``` -ResourceGroupName ```<ResourceGroupName>``` -TemplateFile  .\ADF-Rserver-apache-spark-pipeline.template.json -TemplateParameterFile .\ADF-Rserver-apache-spark-pipeline.parameters.json**
5.	In case of error, you can debug the above command by running it again with '-Debug' switch
6.	If everything goes well then go to the [Azure Portal](https://portal.azure.com), find the resource group and check if the ADF is running properly or not.  


## Expectations:
1.	Check the pipeline run in the diagram view
    a.	Find the Data Factory pipeline and click on the 'Diagram' view. You should be able to see the pipeline with input and output datasets.
    
    b.	Click on the output - 'finalOutputDataset' and now you can check the status of the recent runs (based on the configuration this pipeline should run only once)
    
    c.	Clicking on one of the runs, one can check the stdout and stderr messages from running the job. 
2.	Check the container for the output file
    a.	<<Need to edit the Rscript to save the output>>

## What is happening in the R script? 
Following sequence of steps happens while running the R script:
1.	Set the context for the Apache Spark: number of executors and amount of memory per executor. This configuration is dependent on both the task and the setup of the Spark cluster. 
2.	Read the dataset and convert input dataset to XDF format
3.	Read the dataset in XDF format, create new features and drop duplicate or unimportant fields 
4.	Run linear regression Algorithm 
5.	Save the model 


# Customizing the pipeline
 It’s possible to customize the template used in this post and apply it to other problems. There are three main parts that can be configured as per requirements: Cluster Configuration, R code that is used for the analysis, and the Data Factory setup.  Below we will give a high level idea on customizing each of these.
 
*	Cluster Configuration: We are using only two worker nodes of the D3_V2 types. For the jobs that require more memory or processing power, one can create more worker nodes and/or use more powerful machines. Changes in the cluster configuration should also be reflected in the R script when setting the compute context to allow for efficient resource utilization.  This post talks about Spark tuning with Microsoft R: https://blogs.msdn.microsoft.com/azuredatalake/2016/08/09/rapid-big-data-prototyping-with-microsoft-r-server-on-apache-spark-context-switching-spark-tuning/
```    
{
 		"name": "workernode",
                              "targetInstanceCount": "[parameters('clusterWorkerNodeCount')]",
"hardwareProfile": {
         "vmSize": "Standard_D3_v2"
},
"osProfile": {
           "linuxOperatingSystemProfile": {
                   "username": "[parameters('sshUserName')]",
                   "password": "[parameters('sshPassword')]"
 }
}
```
*Figure 2.* A code snippet from the ARM template showing the configuration of the worker nodes. 'targetInstanceCount' value decides the number of worker nodes (currently set to 'clusterWorkerNodeCount’ as set in the parameter file).The 'vmSize' decides the type of VM used for the worker nodes.
 


*	R script: One can replace/modify the current R script 'RScriptToRun.R' that is kept in the script folder among the files that were copied from the public repository.  For changing the name of the script file or modifying the arguments, changes must be edited in the 'ADFTutorialARM-Rserver-MapReduceRun.json' in the Data Pipeline section. Figure 2 shows the relevant section from the ARM template.

```
"activities": [
  {                             "type": "HDInsightMapReduce",
                                "typeProperties": {
                                    "className": "com.adf.jobonhdi.JobOnHdiLauncher",
                                    "jarFilePath":   "[concat(parameters('parameterStorageContainerName'), '/scripts/com.adf.adfjobonhdi.jar')]",
                                    "jarLinkedService": "[variables('storageLinkedServiceName')]",
                                    "arguments": [
                                        "--files",  
                                       "[concat('wasb://',parameters ('parameterStorageContainerName'),'@', variables('storageAccountName'),
                                            '.blob.core.windows.net/scripts/com.adf.appsample.jar,',' wasb://',parameters('parameterStorageContainerName'),
                                              '@',variables('storageAccountName'),'.blob.core.windows.net/scripts/RScriptToRun.R')]",
                                        "--command",
                                        "[concat('source /usr/lib64/microsoft-r/8.0/hadoop/RevoHadoopEnvVars.site; Revo64 CMD BATCH \"--args ',
                                              '/data/programmers.tsv/ /output/model \" ',variables('rscript-name'), ' /dev/stdout')]"
                                    ]
                                },
```

*Figure 3.* A code snippet from the ARM template showing activity that runs the R script. The name of the script is stored in the variable – ‘rscript-name’. ‘/data/prgrammers.tsv’ and ‘/output/model’ are the arguments to the rscript   The script is run using batch mode provided by R. Note that we use command ‘Revo64’ instead of ‘R’ as we are referring to the R server provided by Microsoft.  


*	Data Factory: Currently the data factory is scheduled to run once a month; one can change that as per the requirements. We are currently having static dataset, but it can be changed to something where new data made accessible to the pipeline. ADF has support for reading partitions defined by time periods. ADF can also utilize data from different sources (including blob storage, databases, etc.).  One can add more activities (such as ETL in Apache Spark) to the pipeline or even create more pipelines. Documentation on [ADF](
https://azure.microsoft.com/en-us/documentation/services/data-factory/) has more details 


```
"policy": {
                                    "timeout": "00:30:00",
                                    "concurrency": 1,
                                    "retry": 1
                                },
                                "scheduler": {
                                    "frequency": "Month",
                                    "interval": 1
                                },
                                "name": "HDInsight Job Launcher",
                                "description": "Submits a general HDInsight Job",
                                "linkedServiceName": "[variables('hdInsightOnDemandLinkedServiceName')]"
                            }
                        ],
                        "start": "2016-04-01T00:00:00Z",
                        "end": "2016-04-02T00:00:00Z",
                        "isPaused": false
```

*Figure 4.* A snippet from the ARM template showing the ‘scheduler’ section that sets the schedule of the pipeline. The 'scheduler' section can be used to configure the pipeline schedule. The ‘start’ and ‘end’ dates decide the time-period in which the pipeline will operate
 


# Conclusion
 
This post highlights how Microsoft R with Spark can be used in production data pipelines for end-to-end data processing and analysis jobs. Integration of Microsoft R with Azure Data factory opens the rich world of open source R packages and brings in the scalability offered by Microsoft R to a controlled and monitored production data pipeline (ADF). Along with the steps we also provide an easily customizable Azure Resource Manager template that allows one-click deployment of the resources (R server, Apache Spark, Azure Data Factory).  

