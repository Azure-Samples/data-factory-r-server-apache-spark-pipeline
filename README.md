--- 
services: data-factory, r-server, hdinsight
platforms: R, Python 
author: udayankumar
---

![Title Image](images/pipelineImage.PNG)

# Introduction
In this tutorial, we highlight how to build a scalable machine learning-based data processing pipeline using [Microsoft R Server](https://www.microsoft.com/en-us/cloud-platform/r-server) with [Apache Spark](https://spark.apache.org/) utilizing [Azure Data Factory](https://azure.microsoft.com/en-us/documentation/articles/data-factory-introduction/) (ADF).  Here, we provide step-by-step instructions and a customizable [Azure Resource Manager template](https://azure.microsoft.com/en-us/documentation/articles/resource-manager-template-walkthrough/) that provides deployment of the entire solution.  
 
A data pipeline with Microsoft R Server makes sense when one uses R for processing large datasets at regular intervals. For e.g., running a stock market analysis model at the end of each business day or re-training/updating the predictive models every hour based on the newly incoming labeled data. If there is a need for an ETL (Extraction, Transformation, and Load) step before the data reaches R, such an ETL step can also be added to the same pipeline as a dependency to the R analytics process. This removes the need for synchronization as R task will wait on the dependencies to finish.
 
As an example dataset for this tutorial, we use open source data, [NYC Taxi Dataset](http://www.andresmh.com/nyctaxitrips), public NYC Taxi Trip and Fare data-set. This dataset contains details on each taxi trip including pick-up/drop-off location, fare, tip paid, etc. Using this data, we want to train a predictive model to predict the tip amount at the beginning of each ride. In the case of NY taxi, newer trip and fare data is generated every day and we want to regularly update our predictive model to incorporate the latest trends. We will build a pipeline that periodically reads the new data and runs it through R to create a new and updated predictive model. We also create a batch scoring pipeline that uses the model created during the training and predicts the outcome (tip amount) over previously unseen data.  
 
This tutorial walk-through emphasizes on how we can build a pipeline using Azure Data factory with Microsoft R over Apache Spark to schedule data processing jobs. We want to give readers a usable example that can be modified for their datasets and use-cases. The main purpose of this blog is to highlight building a datapipeline, and for the sake of simplicity, we have skipped tuning and validation of our machine learning model.  
 
By the end of this tutorial we want readers to be confident in:
 
1.	Utilizing Microsoft R and Spark cluster using Azure Data Factory (ADF) (with Azure Resource Templates) 
2.	Setting up custom R jobs using the ADF
3.	Managing the data movements when using Microsoft R with Spark


# Background 
 
There are three things that we should understand as we divulge into the details:
 
## Microsoft R Server 
Microsoft provides an enhanced implementation on top of Open-source R that allows benefits of distributed computing to flow into R.  The Microsoft R Server implements several popular machine learning algorithms to make them more efficient and scalable over distributed computing frameworks.  Apache Spark is one of the supported distributing computing platforms for Microsoft R.  The use of Apache Spark allows us to do computations on datasets in R that do not fit onto the memory of a single machine. Microsoft R Server is available for both on-premise and cloud-based deployment. In this tutorial, we utilize cloud-based deployment.
 
The beauty of Microsoft R comes from the design that allows with a single line change (setting a suitable computing context) to switch the backend computing platform (from Hadoop to Apache Spark to local machine).  Rest of the code is agnostic and independent to the backend. We mainly use the functionality provided by the [RevoScaleR package](https://msdn.microsoft.com/en-us/microsoft-r/scaler-getting-started). More details on how to use R with Spark are on this [blog](https://blogs.msdn.microsoft.com/azuredatalake/2016/08/09/rapid-big-data-prototyping-with-microsoft-r-server-on-apache-spark-context-switching-spark-tuning/).
 
## Azure Data Factory (ADF)
Data factory allows users to create data pipelines and orchestrates data movement. In simple words, allows users to specify dependencies between data and computational tasks and create a time-based data flow. The usefulness of using ADF comes from the fact that it provides several inbuilt tools for creating a pipeline (on demand clusters, ability to run custom jobs) and provides insights into each step of a running pipeline. The documentation [here](https://azure.microsoft.com/en-us/documentation/services/data-factory/) provides details on ADF  . Similar to other services offered by Azure, ADF also allows one-click deployment through [Azure Resource Manager Templates](https://azure.microsoft.com/en-us/documentation/articles/resource-group-authoring-templates/).  ADF also has support to run Hadoop/Spark cluster as a PAAS and can also integrate with existing HDInsight clusters among other compute platforms. A typical ADF instance comprises of pipelines and each pipeline can contain multiple activities. Each activity takes at least one input dataset, a compute linked services to process the input and produce at least one output dataset. A [compute linked service](https://azure.microsoft.com/en-us/documentation/articles/data-factory-compute-linked-services/) is a facility that allows connecting ADF to computer resources such Spark cluster or Azure Batch. 

> **Notes:** 
1. Currently ADF only supports Map-reduce jobs on Linux HDI clusters and since MRS runs only on Linux HDInsight clusters, as a workaround, we will use a  [custom task-runner](https://github.com/Azure/Azure-DataFactory/tree/master/Samples/CustomizedJavaOnHDInsight) masquerading as a map-reduce job to run MRS script.
2. We also set the CLASSPATH, through the template, for properly running the R Server through ADF using `source /usr/lib64/microsoft-r/8.0/hadoop/RevoHadoopEnvVars.site;` No user action is needed to run this command.  

## Dataset
The 2013 [NYC dataset](http://www.andresmh.com/nyctaxitrips) in the available form has two `csv` files for Trips and Fares. For the predictive use, we process and join them on the same lines as the instruction in this [post](https://azure.microsoft.com/en-us/documentation/articles/machine-learning-data-science-process-hive-walkthrough). Instead of using Hive, we use a [python script](./scripts-used/joinNYC.py) to use spark SQL for the join.  To speed up the demo, we are only using subset of data from the first dataset for training (trip_data_1.csv.zip and trip_fare_1.csv.zip) . The scoring is done on the second dataset (trip_data_2.csv.zip and trip_fare_2.csv.zip).  One can take similar steps to run this demo code on larger datasets. 

In the following sections, we provide steps to create an ADF pipeline with Microsoft R to for generating a machine learning model on the NYC taxi dataset.  


# Setup

1.	Create an [Azure account](https://azure.microsoft.com/en-us/) (Skip if already have an azure account)
2.	Setup any one of [several available template deployment methods](https://azure.microsoft.com/en-us/documentation/articles/resource-group-template-deploy/). 
It's also possible to deploy templates through [Azure portal](https://azure.microsoft.com/en-us/documentation/articles/resource-group-template-deploy-portal/#deploy-resources-from-custom-template), minimizing the setup time. We use Azure [PowerShell](https://azure.microsoft.com/en-us/documentation/articles/powershell-install-configure/) based deployment through this blog.

3.	Before starting the pipeline, we must make the dataset available to the R-server/Spark cluster. Every Spark Cluster on Azure is associated with at least one storage account. Below are the steps using Azure [PowerShell](https://azure.microsoft.com/en-us/documentation/articles/powershell-install-configure/) on how to create a Storage Account and then make the dataset/scripts available on this storage account.

     a. Download the template [Storage-account-data-copy-setup.template.json](Storage-account-data-copy-setup.template.json). After deployment, this template will create a new storage account and copy data to this storage account using copy functionality of Azure Data Factory (ADF). Once downloaded, navigate to template's location using PowerShell to begin its deplyoment 
     
     b. Login to Azure using PowerShell 

       ```csharp
       > Login-AzureRmAccount
       ```

     c. List all available subscription

       ```csharp
       > Get-AzureRmSubscription
       ```

     d. Select an Azure subscription to run this tutorial
     
       ```csharp
        > Select-AzureRmSubscription  -SubscriptionId <Subscription ID>
       ```

    e. Create a Resource Group. Resouce group name can be any string. Use this same Resource Group Name throughout the tutorial. Location too can be modifed dependent on the resource availability. We have selected `South Central US` 
       
       ```csharp
        > New-AzureRmResourceGroup -Name <ResourceGroupName> -Location "South Central US"
       ```

    f. Deploy the Data copying ARM template that will create the storage account and  copy dataset from public repo to this storage Account
       
      ```csharp
       > New-AzureRmResourceGroupDeployment -Name <deploymentName> -ResourceGroupName <ResourceGroupName> -TemplateFile .\Storage-account-data-copy-setup.template.json
      ```

    g. After a successful deployment, storage account name - `parameterStorageAccountName`, storage account keys - `parameterStorageAccountKey` and the container name - `parameterStorageContainerName` will be printed as output. For the deployment of the training/scoring pipelines, we will use these values to connect the downloaded dataset with R/Spark cluster.          


4.	Download [ADF-Rserver-apache-spark-pipeline.template.json](ADF-Rserver-apache-spark-pipeline.template.json) and [ADF-Rserver-apache-spark-pipeline.parameters.json](ADF-Rserver-apache-spark-pipeline.parameters.json) .  The first file is the Azure Resource Management template file that contains information on the resources and the constraints. The second file is a parameter file that contains all the configuration parameters including passwords and keys. Edit this parameter file and replace `parameterStorageAccountName`, `parameterStorageAccountAccessKey`, and `parameterStorageContainerName` with the values from step 3g. We also strongly recommend changing the passwords (10 characters long with at least a digit, a special character, a lower-case alphabet, and an upper-case alphabet).  



# Starting the pipeline
 
Azure allows deploying resources and configurations through [Azure Resource Management templates](https://azure.microsoft.com/en-us/documentation/articles/resource-manager-template-walkthrough/). We will show steps for  template based deployment. The template will start the HDInsight’s Spark Cluster, install Microsoft R server, and finally create Azure data factory. Once the resource deployment is complete, the pipeline will start running based on the temporal configurations (currently set to once a month). The advantage of template deployment is speed of deployment and repeatability. For our current scenario, we store the configuration parameters such as the ssh username/password, in a separate parameter file that is passed along with the template for deployment. Below are the steps for deploying the template using Windows PowerShell and what to expect after deployment. 

![Relationship among Azure Data Factory component](images/ADF-flow-diagram.PNG)

*Figure 1.* Relationship among Azure Data Factory component

## Deployment
1.  Login to Azure using PowerShell 

    ```csharp
     > Login-AzureRmAccount
     ```
2. List all available subscription

    ```csharp
     > Get-AzureRmSubscription
    ```

3. Select an Azure subscription

    ```chsarp
     > Select-AzureRmSubscription  -SubscriptionId <Subscription ID>
    ```
    
4. Using the Resource Group name used for creating the Storage Account in the *Setup* section, deploy the arm template. The 'NameForTheDeployment' can be any string to identify the deployment

    ```csharp
      > New-AzureRmResourceGroupDeployment -Name <DeploymentName> -ResourceGroupName <ResourceGroupName> -TemplateFile  .\ADF-Rserver-apache-spark-pipeline.template.json -TemplateParameterFile .\ADF-Rserver-apache-spark-pipeline.parameters.json
     ```
5. This deployment step will take approximately 40 minutes. In case of an error, you can debug the above command by running it again with '-Debug' switch

6. If everything goes well then go to the [Azure Portal](https://portal.azure.com), find the resource group and check the status of ADF pipelines


## Expectations and further steps
1. Check the pipeline run in the diagram view

    a.	Find the Data Factory pipeline and click on the `Diagram` view. You should be able to see two pipelines (training and scoring) with input and output datasets.
    
    b.	Click on the output - `finalOutputDatasetTraining` and now you can check the status of the recent runs for the training pipeline (based on the configuration this pipeline should run only once)
    
    c.	Clicking on one of the runs, one can check the stdout and stderr messages from running the job. 
  
    d. If the status is `Succeeded`, then the attached storage container has a file named 'model'. This file contains the model object that can be used for prediction tasks. Now we can start the scoring pipeline

2.	Start the scoring pipeline

    a. By default, the scoring pipeline is set to pause. This is because it needs the 'model' file, which is generated upon the first run of the training pipeline. And since the scoring pipeline's running frequency is independent of the training pipeline, we have not added any dependencies between them. Therefore, the scoring pipeline is paused and we manually start it when the 'model' file is available.  

    b. To start the scoring pipeline, we need to change a config parameter for that pipeline through [portal](https://portal.azure.com). Find the Data Factory that we created earlier and click - `Author and Deploy`, then expand `Pipelines` and click on `datafactoryscoring` pipeline. Towards the bottom, there is a key - `isPaused`, change the value for this key from `true` to `false`. Now click on `Deploy` button to activate these changes.

    c. Now go back to the `Diagram` view and click the `finalOutputDatasetScoring` dataset, check the runs, if they have still not started, you can manually start the run (generally it take a moment before the pipeline starts). Now once the run is complete, similar to the training pipeline, one can check the status of the pipeline run.


## Sequence of steps in the R script for training 

Following sequence of steps happens while running the training  R script:

1.	Set the context for the Apache Spark: number of executors and amount of memory per executor. This configuration is dependent on both the task and the setup of the Spark cluster. 
2.	Read the dataset and convert input dataset to XDF format
3.	Read the dataset in XDF format, create new features and drop duplicate or unimportant fields 
4.	Run Linear-Regression algorithm 
5.	Save the model 


# Customizing the pipeline
 It’s possible to customize the template used in this tutorial and apply it to other problems. There are three main parts that can be configured as per requirements: Cluster Configuration, R code that is used for the analysis, and the Data Factory setup.  Below we will give a high level idea on customizing each of these.
 
*	Cluster Configuration: We are using only two worker nodes of the D3_V2 types. For the jobs that require more memory or processing power, one can create more worker nodes and/or use more [powerful machines](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-windows-sizes/). Changes in the cluster configuration should also be reflected in the R script when setting the compute context to allow for efficient resource utilization.  This [tutorial](https://blogs.msdn.microsoft.com/azuredatalake/2016/08/09/rapid-big-data-prototyping-with-microsoft-r-server-on-apache-spark-context-switching-spark-tuning/) talks about Spark tuning with Microsoft R.
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
*Figure 2.* A code snippet from the ARM template showing configuration of the worker nodes. 'targetInstanceCount' value decides the number of worker nodes (currently set to 'clusterWorkerNodeCount’ as set in the parameter file).The 'vmSize' decides the type of VM used for the worker nodes.
 


*	R script: One can replace/modify the R scripts too, for example the training pipeline runs the script in file `RScriptForNYCTaxi.R`. This R script is kept in the script folder among the files that were copied from the public repository. This script's name is stored in the template variable - `rscript-name`.  Editing the value for this variable will allow running a different script. The modifications to the arguments must be made in the Data Pipeline section of [ADF-Rserver-apache-spark-pipeline.template.json](ADF-Rserver-apache-spark-pipeline.template.json). Figure 2 shows the relevant section from the ARM template.

```
"activities": [
  {                             "type": "HDInsightMapReduce",
                                "typeProperties": {
                                    "className": "com.adf.jobonhdi.JobOnHdiLauncher",
                                    "jarFilePath":   "[concat(parameters('parameterStorageContainerName'), '/scripts/com.adf.adfjobonhdi.jar')]",
                                    "jarLinkedService": "[variables('storageLinkedServiceName')]",
                                    "arguments": [
                                        "--files",  
                                        "[concat('wasb://',parameters('parameterStorageContainerName'),'@', variables('storageAccountName'),
                                            '.blob.core.windows.net/scripts/com.adf.appsample.jar,',' wasb://',parameters('parameterStorageContainerName'),
                                            '@',variables('storageAccountName'),'.blob.core.windows.net/scripts/',variables('rscript-name'))]",
                                        "--command",
                                        "[concat('source /usr/lib64/microsoft-r/8.0/hadoop/RevoHadoopEnvVars.site; Revo64 CMD BATCH \"--args ',
                                              '/data/nyc_taxi_joined_csv/ / \" ',variables('rscript-name'), ' /dev/stdout')]"
                                    ]
                                },
```

*Figure 3.* A code snippet from the ARM template showing activity that runs the R script. The name of the script is stored in the variable – ‘rscript-name’. ‘/data/nyc_taxi_joined_csv/’ and ‘/output/model/’ are the arguments to the R script.   The script is run using batch mode provided by R. Note that we use command ‘Revo64’ instead of ‘R’ as we are referring to the R server provided by Microsoft.  


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
 
This tutorial highlights how Microsoft R Server with Spark can be used in production data pipelines for end-to-end data processing and machine-learning jobs. Integration of Microsoft R with Azure Data factory opens the rich world of open source R packages and brings in the scalability offered by Microsoft R to a controlled and monitored production data pipeline (ADF). Along with the steps we also provide an easily customizable Azure Resource Manager template that allows one-click deployment of the resources (Microsoft R server, Apache Spark, Azure Data Factory).  

